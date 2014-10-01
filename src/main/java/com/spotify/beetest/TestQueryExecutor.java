package com.spotify.beetest;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.lang.IllegalArgumentException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.cli.ParseException;
import junitx.framework.FileAssert;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author kawaa
 */
public class TestQueryExecutor {

    private static final Logger LOGGER =
            Logger.getLogger(TestQueryExecutor.class.getName());
    private static boolean deleteBeetestTestDir = true;

    private static String getTestCaseCommand(String config, String queryFilename, Properties variables) {
        List<String> args = new ArrayList<String>(Arrays.asList("hive", "--config", config, "-f", queryFilename));
        for (String key :  variables.stringPropertyNames()) {
            args.add("--hivevar");
            args.add(key + "=" + variables.get(key));
        }
        return StringUtils.join(args, " ");
    }

    public static void run(String testCase, String config) throws IOException, InterruptedException {

        TestCase tc = new TestCase(testCase);

        String queryFilename = tc.generateTestCaseQueryFile();

        LOGGER.log(Level.INFO, "Generated query filename: {0}", queryFilename);
        LOGGER.log(Level.INFO, "Generated query content: \n{0}", tc.getBeeTestQuery());

        Properties variables = new Properties();
        try {
            variables.load(new FileInputStream(tc.getVariablesFilename()));
        } catch (IOException ex) {
            LOGGER.log(Level.INFO, "Missing variables file");
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.SEVERE, "Invalid variables file");
        }
        String testCaseCommand = getTestCaseCommand(config, queryFilename, variables);

        LOGGER.log(Level.INFO, "Running: {0}", testCaseCommand);

        Utils.runCommand(testCaseCommand, LOGGER);

        LOGGER.log(Level.INFO, "Asserting: {0} and {1}",
                new Object[]{tc.getExpectedFilename(), tc.getOutputFilename()});

        FileAssert.assertEquals("Output does not match",
                new File(tc.getExpectedFilename()),
                new File(tc.getOutputFilename()));

        if (deleteBeetestTestDir) {
	// TODO: some cleanup
        }
    }

    public static void main(String[] args)
            throws ParseException, IOException, InterruptedException {

        String testCase = args[0];
        String config = args[1];

        run(testCase, config);
    }
}
