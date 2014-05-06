package com.spotify.beetest;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
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

    private static String getTestCaseCommand(String config, String queryFilename) {
        return StringUtils.join("hive --config ", config, " -f ", queryFilename);
    }

    public static void run(String testCase, String config) throws IOException, InterruptedException {

        TestCase tc = new TestCase(testCase);

        String queryFilename = tc.generateTestCaseQueryFile();

        LOGGER.log(Level.INFO, "Generated query filename: {0}", queryFilename);
        LOGGER.log(Level.INFO, "Generated query content: \n{0}", tc.getBeeTestQuery());

        String testCaseCommand = getTestCaseCommand(config, queryFilename);

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
