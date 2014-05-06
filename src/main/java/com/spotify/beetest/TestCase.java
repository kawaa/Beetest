package com.spotify.beetest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

public final class TestCase {

    private String ddlTableFilename;
    private String setupQueryFilename;
    private String selectQueryFilename;
    private String expectedFilename;
    private String outputDirectory;
    private String outputTable = "output";
    private String testDirectory;
    private String databaseName = "beetest";
    private String testCaseQueryFilename = StringUtils.join(
            "/tmp/beetest-query-", Utils.getRandomPositiveNumber(), ".hql");
    private static String NL = "\n";
    private static String TAB = "\t";

    public TestCase() {
    }

    public TestCase(String ddlSetupFilename, String queryFilename, String selectFilename,
            String expectedFilename, String outputDir, String testDirectory) {
        this.ddlTableFilename = ddlSetupFilename;
        this.selectQueryFilename = selectFilename;
        this.setupQueryFilename = queryFilename;
        this.expectedFilename = expectedFilename;
        this.outputDirectory = outputDir;
        this.testDirectory = testDirectory;
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
        if (new File(StringUtils.join(directory, "/table.ddl")).exists()) {
            ddlTableFilename = StringUtils.join(directory, "/table.ddl");
        }
        if (new File(StringUtils.join(directory, "/setup.hql")).exists()) {
            setupQueryFilename = StringUtils.join(directory, "/setup.hql");
        }
        selectQueryFilename = StringUtils.join(directory, "/select.hql");
        expectedFilename = StringUtils.join(directory, "/expected");
        outputDirectory = StringUtils.join(directory, "/", outputTable);
        testDirectory = directory;
    }

    private void setupFromFile(String filename) throws IOException {
        Properties prop = new Properties();
        //load a properties file
        prop.load(new FileInputStream(filename));
        ddlTableFilename = prop.getProperty("t");
        selectQueryFilename = prop.getProperty("st");
        setupQueryFilename = prop.getProperty("sp");
        expectedFilename = prop.getProperty("e");
        outputDirectory = prop.getProperty("o");

    }

    public String getExpectedFilename() {
        return expectedFilename;
    }

    public String getOutputDirectory() {
        return outputDirectory;
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

        String ctas = StringUtils.join("CREATE TABLE ", outputTable, NL,
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ", NL,
                "LOCATION '", outputDirectory, "' AS ", NL);
        String select = Utils.readFile(selectFilename);
        return ctas + select;
    }

    public String getBeeTestQuery() throws IOException {
        // own database
        String databaseQuery = StringUtils.join("CREATE DATABASE IF NOT EXISTS ",
                databaseName, ";", NL, "USE ", databaseName, ";", NL);

        // setup
        String tableDdl = (ddlTableFilename != null
                ? getDDLSetupQuery(ddlTableFilename, testDirectory) : "");
        String query = (setupQueryFilename != null
                ? Utils.readFile(setupQueryFilename) : "");

        // final query
        return StringUtils.join(databaseQuery, tableDdl, query,
                getTestedQuery(outputTable, outputDirectory, selectQueryFilename));
    }

    public String getOutputFilename() {
        return getOutputDirectory() + "/000000_0";
    }

    public String getTestCaseQueryFilename() {
        return testCaseQueryFilename;
    }

    public String generateTestCaseQueryFile()
            throws FileNotFoundException, UnsupportedEncodingException,
            IOException {
        generateTextFile(testCaseQueryFilename, getBeeTestQuery());
        return testCaseQueryFilename;
    }

    public boolean deleteTestCaseQueryFile() {
        File file = new File(testCaseQueryFilename);
        return file.delete();
    }

    private String generateTextFile(String filename, String content)
            throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(filename, "UTF-8");
        writer.println(content);
        writer.close();
        return filename;
    }
}
