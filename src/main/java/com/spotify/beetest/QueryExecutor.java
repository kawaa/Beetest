/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spotify.beetest;

import java.io.IOException;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author kawaa
 */
public class QueryExecutor {

    public static void run(String testCaseFilename, String config)
            throws IOException, InterruptedException {
        TestCase tc = new TestCase(testCaseFilename);
        String query = tc.getFinalQuery();

        String queryFilename = "/tmp/beetest-query-"
                + Utils.getRandomNumber() + ".hql";

        Utils.createTextFile(queryFilename, query);
        Utils.runCommand("hive --config " + config + " -f " + queryFilename);
        Utils.deleteFile(queryFilename);

        String diff = "diff " + tc.getExpectedFilename()
                + " " + tc.getOutputDirectory() + "/*";
        Utils.runCommand(diff);
    }

    public static void main(String[] args)
            throws ParseException, IOException, InterruptedException {
        String testCaseFilename = args[0];
        String config = args[1];
        run(testCaseFilename, config);
    }
}
