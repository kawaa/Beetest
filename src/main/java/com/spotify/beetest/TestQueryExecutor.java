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

    private static final Logger LOGGER = Logger.getLogger(TestQueryExecutor.class.getName());
    private static boolean deleteTestCaseQueryFile = true;
    
    private static String getTestCaseCommand(String config, String queryFilename) {
        return StringUtils.join("hive --config ", config, " -f ", queryFilename);
    }

    public static void run(String testCase, String config, String beetestDir)
            throws IOException, InterruptedException {
        
        if (beetestDir != null) {
            LOGGER.log(Level.INFO, "Removing a directory: {0}", beetestDir);
            Utils.deletePath(beetestDir);
        }

        TestCase tc = new TestCase(testCase);
        
        String queryFilename = tc.generateTestCaseQueryFile();        
        
        LOGGER.log(Level.INFO, "Generated query filename: {0}", queryFilename);
        LOGGER.log(Level.INFO, "Generated query content: \n{0}", tc.getFinalQuery());

        String testCaseCommand = getTestCaseCommand(config, queryFilename);
        LOGGER.log(Level.INFO, "Running: {0}", testCaseCommand);
        Utils.runCommand(testCaseCommand, LOGGER);

        LOGGER.log(Level.INFO, "Asserting: {0} and {1}",
                new Object[]{tc.getExpectedFilename(), tc.getOutputFilename()});
        
        FileAssert.assertEquals(new File(tc.getExpectedFilename()),
                new File(tc.getOutputFilename()));
        
        if (deleteTestCaseQueryFile) {
            tc.deleteTestCaseQueryFile();
        }
    }
    
    public static void main(String[] args)
            throws ParseException, IOException, InterruptedException {
        
        String testCase = args[0];
        String config = args[1];
        String beetestDir = (args.length > 2) ? args[2] : null;
        
        run(testCase, config, beetestDir);
    }
}