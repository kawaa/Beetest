package com.spotify.beetest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

public final class TestCase {

    private String ddlSetupFilename;
    private String queryFilename;
    private String selectFilename;
    private String expectedFilename;
    private String outputDirectory;
    private String outputTable = "output";

    private String databaseName = "beetest";
    private String testCaseQueryFilename = StringUtils.join(
            "/tmp/beetest-query-", Utils.getRandomPositiveNumber(), ".hql");
    private static String NL = "\n";
    private static String TAB = "\t";

    public TestCase() {
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
        if (new File(StringUtils.join(directory, "/setup.ddl")).exists()) {
            ddlSetupFilename = StringUtils.join(directory, "/setup.ddl");
        }
        if (new File(StringUtils.join(directory, "/query.hql")).exists()) {
            queryFilename = StringUtils.join(directory, "/query.hql");
        }
        selectFilename = StringUtils.join(directory, "/select.hql");
        expectedFilename = StringUtils.join(directory, "/expected.tsv");
        outputDirectory = StringUtils.join(directory, "/output");
    }

    private void setupFromFile(String filename) throws IOException {
        Properties prop = new Properties();
        //load a properties file
        prop.load(new FileInputStream(filename));
        ddlSetupFilename = prop.getProperty("ds");
        selectFilename = prop.getProperty("s");
        queryFilename = prop.getProperty("q");
        expectedFilename = prop.getProperty("e");
        outputDirectory = prop.getProperty("o");

    }

    public TestCase(String ddlSetupFilename, String queryFilename, String selectFilename,
            String expectedFilename, String outputDir) {
        this.ddlSetupFilename = ddlSetupFilename;
        this.selectFilename = selectFilename;
        this.queryFilename = queryFilename;
        this.expectedFilename = expectedFilename;
        this.outputDirectory = outputDir;
    }

    public String getExpectedFilename() {
        return expectedFilename;
    }

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public String getDDLSetupQuery(String ddlSetupFilename)
            throws IOException {
        StringBuilder query = new StringBuilder();

        List<String> fileContent = Utils.fileToList(ddlSetupFilename);
        for (String line : fileContent) {
            String[] parts = line.split(TAB);
            String tableName = parts[0];
            String tableSchema = parts[1];
            String inputPath = parts[2];

            String initTable = StringUtils.join("DROP TABLE IF EXISTS ", tableName, ";",
                    NL, "CREATE TABLE ", tableName, tableSchema,
                    NL, "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';",
                    NL, "LOAD DATA LOCAL INPATH '", inputPath, "' INTO TABLE ",
                    tableName, ";", NL);
            query.append(initTable);
        }

        return query.toString();
    }

    public String getTestedQuery(String outputTable, String outputDirectory,
            String selectFilename) throws IOException {

        String ctas = StringUtils.join("DROP TABLE IF EXISTS ", outputTable, ";",
                    NL, "CREATE TABLE ", outputTable,
                    NL, "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ",
                    NL, "LOCATION '", outputDirectory, "' AS ",
                    NL);
        String select = Utils.readFile(selectFilename);
        return ctas + select;
    }

    public String getFinalQuery() throws IOException {
        // own database
        String databaseQuery = StringUtils.join("CREATE DATABASE IF NOT EXISTS ",
                databaseName, ";", NL, "USE ", databaseName, ";", NL);

        // setup
        String ddlSetup = (ddlSetupFilename != null
                ? getDDLSetupQuery(ddlSetupFilename) : "");
        String query = (queryFilename != null
                ? Utils.readFile(queryFilename) : "");

        // final query
        return StringUtils.join(databaseQuery, ddlSetup, query,
                getTestedQuery(outputTable, outputDirectory, selectFilename));
    }

    public Options getOptions() {
        Options options = new Options();
        options.addOption("ds", true, "specify a DDL setup file");
        options.addOption("s", true, "specify a select file");
        options.addOption("q", true, "specify a query file");
        options.addOption("e", true, "specify an expected output file");
        options.addOption("o", true, "specify an output directory");

        return options;
    }

    public boolean parseOptions(String[] args) throws ParseException {
        boolean validArgs = true;
        // create Options object
        Options options = getOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("ds")) {
            ddlSetupFilename = cmd.getOptionValue("ds");
        }
        if (cmd.hasOption("q")) {
            queryFilename = cmd.getOptionValue("q");
        }

        if (cmd.hasOption("s")) {
            selectFilename = cmd.getOptionValue("s");
        } else {
            System.err.println("Option -s (selectFilename) is mandatory");
            validArgs = false;
        }
        if (cmd.hasOption("e")) {
            expectedFilename = cmd.getOptionValue("e");
        } else {
            System.err.println("Option -e (expectedFilename) is mandatory");
            validArgs = false;
        }
        if (cmd.hasOption("o")) {
            outputDirectory = cmd.getOptionValue("o");
        } else {
            System.err.println("Option -o (outputDir) is mandatory");
            validArgs = false;
        }

        return validArgs;
    }

    public String run(String[] args) throws ParseException, IOException {
        TestCase qg = new TestCase();
        if (qg.parseOptions(args)) {
            return qg.getFinalQuery();
        } else {
            return null;
        }
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
        generateTextFile(testCaseQueryFilename, getFinalQuery());
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

    public static void main(String[] args) throws ParseException, IOException {
        System.out.print(new TestCase().run(args));
    }
}
