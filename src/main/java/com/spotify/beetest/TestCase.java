package com.spotify.beetest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.commons.lang3.text.StrLookup;


class ExternalFilesSubstitutor {
    public static final String replace(final String source) {
        StrSubstitutor strSubstitutor = new StrSubstitutor(
                new StrLookup<Object>() {
                    @Override
                    public String lookup(final String key) {
                        try {
                            return Utils.readFile(StringUtils.trim(key));
                        } catch (IOException e) {
                            return "";
                        }
                    }
                }, "<%", "%>", '$');
        return strSubstitutor.replace(source);
    }
}


public final class TestCase {

    private String ddlTableFilename;
    private String setupQueryFilename;
    private String selectQueryFilename;
    private String expectedFilename;
    private String testDirectory;
    private String variablesFilename;
    private int TEST_ID = Utils.getRandomPositiveNumber();
    private String DATABASE_NAME = "beetest";
    private String BEETEST_TEST_DIR = StringUtils.join(
            "/tmp/beetest-test-", TEST_ID);
    private String BEETEST_TEST_QUERY = StringUtils.join(
            BEETEST_TEST_DIR, "-query.hql");
    private String BEETEST_TEST_OUTPUT_TABLE = "output_" + TEST_ID;
    private String BEETEST_TEST_OUTPUT_DIRECTORY = StringUtils.join(
            BEETEST_TEST_DIR, "-", BEETEST_TEST_OUTPUT_TABLE);
    private static String NL = "\n";
    private static String TAB = "\t";

    public TestCase() {
    }

    public TestCase(String ddlSetupFilename, String queryFilename, String selectFilename,
            String expectedFilename, String testDirectory, String variablesFilename) {
        this.ddlTableFilename = ddlSetupFilename;
        this.selectQueryFilename = selectFilename;
        this.setupQueryFilename = queryFilename;
        this.expectedFilename = expectedFilename;
        this.testDirectory = testDirectory;
        this.variablesFilename = variablesFilename;
    }

    public TestCase(String path) throws IOException {
        boolean isDirectory = (new File(path)).isDirectory();
        if (isDirectory) {
            setupFromDirectory(path);
        } else {
            setupFromFile(path);
        }
    }

    private void setupFromDirectory(String directory) {
        testDirectory = directory;
        if (new File(StringUtils.join(directory, "/table.ddl")).exists()) {
            ddlTableFilename = StringUtils.join(directory, "/table.ddl");
        }
        if (new File(StringUtils.join(directory, "/setup.hql")).exists()) {
            setupQueryFilename = StringUtils.join(directory, "/setup.hql");
        }
        selectQueryFilename = StringUtils.join(directory, "/select.hql");
        expectedFilename = StringUtils.join(directory, "/expected.txt");
        variablesFilename = StringUtils.join(directory, "/variables.properties");
    }

    private void setupFromFile(String filename) throws IOException {
        Properties prop = new Properties();
        //load a properties file
        prop.load(new FileInputStream(filename));
        ddlTableFilename = prop.getProperty("t");
        selectQueryFilename = prop.getProperty("st");
        setupQueryFilename = prop.getProperty("sp");
        expectedFilename = prop.getProperty("e");
        variablesFilename = prop.getProperty("v");
    }

    public String getExpectedFilename() {
        return expectedFilename;
    }

    public String getVariablesFilename() {
        return variablesFilename;
    }


    public String getDDLSetupQuery(String ddlSetupFilename, String testDirectory)
            throws IOException {
        StringBuilder query = new StringBuilder();

        List<String> fileContent = Utils.fileToList(ddlSetupFilename);
        for (String line : fileContent) {

            String[] parts = line.split(TAB);
            String table = parts[0];
            String tableName = table.substring(0, table.indexOf("("));
            String tableSchema = table.substring(table.indexOf("(") + 1, table.indexOf(")"));

            String createTable = StringUtils.join(
                    "DROP TABLE IF EXISTS ", tableName, ";", NL,
                    "CREATE TABLE ", tableName, "(", tableSchema, ")", NL,
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';", NL);
            query.append(createTable);

            if (parts.length == 1) {
                String file = testDirectory + "/" + tableName + ".txt";
                String load = StringUtils.join("LOAD DATA LOCAL INPATH '", file,
                        "' INTO TABLE ", tableName, ";", NL);
                query.append(load);
            } else if (!((parts.length == 2) && (parts[1].equals(" ")))) {
                for (int i = 1; i < parts.length; ++i) {
                    String load = StringUtils.join("LOAD DATA LOCAL INPATH '", parts[i],
                            "' INTO TABLE ", tableName, ";", NL);
                    query.append(load);
                }
            }
        }

        return query.toString();
    }

    public String getTestedQuery(String outputTable, String outputDirectory,
            String selectFilename) throws IOException {

        String ctas = StringUtils.join("DROP TABLE IF EXISTS ", outputTable, ";", NL,
		"CREATE TABLE ", outputTable, NL,
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ", NL,
                "LOCATION '", outputDirectory, "' AS ", NL);
        String select = Utils.readFile(selectFilename);
        return ctas + select;
    }

    public String getBeeTestQuery() throws IOException {
        // own database
        String databaseQuery = StringUtils.join("CREATE DATABASE IF NOT EXISTS ",
                DATABASE_NAME, ";", NL, "USE ", DATABASE_NAME, ";", NL);

        // setup
        String tableDdl = (ddlTableFilename != null
                ? getDDLSetupQuery(ddlTableFilename, testDirectory) : "");
        String query = (setupQueryFilename != null
                ? Utils.readFile(setupQueryFilename) : "");

        // include external files in setup.hql
        query = ExternalFilesSubstitutor.replace(query);
        // final query
        return StringUtils.join(databaseQuery, tableDdl, query,
                getTestedQuery(BEETEST_TEST_OUTPUT_TABLE, BEETEST_TEST_OUTPUT_DIRECTORY, selectQueryFilename));
    }

    public String getOutputFilename() {
        return BEETEST_TEST_OUTPUT_DIRECTORY + "/000000_0";
    }

    public String getTestCaseQueryFilename() {
        return BEETEST_TEST_QUERY;
    }

    public String generateTestCaseQueryFile()
            throws FileNotFoundException, UnsupportedEncodingException,
            IOException {
        generateTextFile(BEETEST_TEST_QUERY, getBeeTestQuery());
        return BEETEST_TEST_QUERY;
    }

    private String generateTextFile(String filename, String content)
            throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(filename, "UTF-8");
        writer.println(content);
        writer.close();
        return filename;
    }
}
