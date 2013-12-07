package com.spotify.beetest;

import java.io.IOException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class QueryGenerator {

    private String table;
    private boolean dropInputTable = true;
    private String setupFilename;
    private String queryFilename;
    private String inputFilename;
    private String expectedFilename;
    private String outputDir;

    public String getSetupQuery(String tableName, String setupFilename,
            boolean dropInputTable) throws IOException {

        StringBuilder query = new StringBuilder();
        if (dropInputTable) {
            query.append("DROP TABLE IF EXISTS ");
            query.append(tableName);
            query.append(";\n");
        }

        String fileContent = Utils.readFile(setupFilename);
        query.append(fileContent);

        return query.toString();
    }

    public String getTestedQuery(String tableName, String outputDir,
            String queryFilename) throws IOException {

        StringBuilder query = new StringBuilder();

        query.append("INSERT OVERWRITE LOCAL DIRECTORY '");
        query.append(outputDir);
        query.append("' \n");

        String fileContent = Utils.readFile(queryFilename);
        query.append(fileContent);

        return query.toString();
    }

    public String getDataLoadQuery(String tableName, String inputFilename)
            throws IOException {
        return "LOAD DATA LOCAL INPATH '" + inputFilename
                + "' OVERWRITE INTO TABLE " + tableName + ";\n";
    }

    public String getFinalQuery() throws IOException {
        return getSetupQuery(table, setupFilename, dropInputTable)
                + getDataLoadQuery(table, inputFilename)
                + getTestedQuery(table, outputDir, queryFilename);
    }

    public Options getOptions() {
        Options options = new Options();
        options.addOption("t", true, "specify a table");
        options.addOption("d", true, "delete table if exists");
        options.addOption("s", true, "specify a setup file");
        options.addOption("i", true, "specify an input file");
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
        if (cmd.hasOption("t")) {
            table = cmd.getOptionValue("t");
        } else {
            System.err.println("Option t (table) is mandatory");
            validArgs = false;
        }
        if (cmd.hasOption("d")) {
            dropInputTable = Boolean.parseBoolean(cmd.getOptionValue("d"));
        }
        if (cmd.hasOption("s")) {
            setupFilename = cmd.getOptionValue("s");
            System.out.println(setupFilename);
        } else {
            System.err.println("Option -s (setupFilename) is mandatory");
            validArgs = false;
        }
        if (cmd.hasOption("i")) {
            inputFilename = cmd.getOptionValue("i");
        } else {
            System.err.println("Option -i (inputFilename) is mandatory");
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
            outputDir = cmd.getOptionValue("o");
        } else {
            System.err.println("Option -o (outputDir) is mandatory");
            validArgs = false;
        }

        return validArgs;
    }

    public String run(String[] args) throws ParseException, IOException {
        QueryGenerator qg = new QueryGenerator();
        if (qg.parseOptions(args)) {
            return qg.getFinalQuery();
        } else {
            return null;
        }
    }

    public static void main(String[] args) throws ParseException, IOException {
        System.out.print(new QueryGenerator().run(args));
    }
}