/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spotify.beetest;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.ParseException;
import junitx.framework.FileAssert;

/**
 *
 * @author kawaa
 */
public class QueryExecutor {

    private static final Logger LOGGER = Logger.getLogger(QueryExecutor.class.getName());

    public static void run(String testCaseFilename, String config)
            throws IOException, InterruptedException {

        TestCase tc = new TestCase(testCaseFilename);
        String query = tc.getFinalQuery();
        String queryFilename = tc.getTestCaseQueryFilename();
        tc.generateTestCaseQueryFile();
        LOGGER.log(Level.INFO, "Generated query filename: {0}", queryFilename);
        LOGGER.log(Level.INFO, "Generated query content: {0}", query);

        String testCaseCommand = "hive --config " + config + " -f " + queryFilename;
        LOGGER.log(Level.INFO, "Running: {0}", testCaseCommand);
        Utils.runCommand(testCaseCommand);

        LOGGER.log(Level.INFO, "Asserting: {0} and {1}",
                new Object[]{tc.getExpectedFilename(), tc.getOutputFilename()});
        FileAssert.assertEquals(new File(tc.getExpectedFilename()),
                new File(tc.getOutputFilename()));

        Utils.deleteFile(queryFilename);
    }

    public static void main(String[] args)
            throws ParseException, IOException, InterruptedException {
        String testCaseFilename = args[0];
        String config = args[1];
        run(testCaseFilename, config);
    }
}