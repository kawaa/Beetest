package com.spotify.beetest;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class TestCase {

    private String setupFilename;
    private String queryFilename;
    private String expectedFilename;
    private String outputDirectory;

    public TestCase() {
    }

    public TestCase(String filename) throws IOException {
        Properties prop = new Properties();

        //load a properties file
        prop.load(new FileInputStream(filename));

        setupFilename = prop.getProperty("s");
        queryFilename = prop.getProperty("q");
        expectedFilename = prop.getProperty("e");
        outputDirectory = prop.getProperty("o");
    }

    public TestCase(String setupFilename, String queryFilename,
            String expectedFilename, String outputDir) {
        this.setupFilename = setupFilename;
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

    public String getSetupQuery(String setupFilename)
            throws IOException {
        return Utils.readFile(setupFilename);
    }

    public String getTestedQuery(String outputDir,
            String queryFilename) throws IOException {

        StringBuilder query = new StringBuilder();

        query.append("INSERT OVERWRITE LOCAL DIRECTORY '");
        query.append(outputDir);
        query.append("' \n");

        String fileContent = Utils.readFile(queryFilename);
        query.append(fileContent);

        return query.toString();
    }

    public String getFinalQuery() throws IOException {
        return getSetupQuery(setupFilename)
                + getTestedQuery(outputDirectory, queryFilename);
    }

    public Options getOptions() {
        Options options = new Options();
        options.addOption("s", true, "specify a setup file");
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
        if (cmd.hasOption("s")) {
            setupFilename = cmd.getOptionValue("s");
        } else {
            System.err.println("Option -s (setupFilename) is mandatory");
            validArgs = false;
        }
        if (cmd.hasOption("q")) {
            queryFilename = cmd.getOptionValue("q");
        } else {
            System.err.println("Option -q (queryFilename) is mandatory");
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

    public static void main(String[] args) throws ParseException, IOException {
        System.out.print(new TestCase().run(args));
    }
}