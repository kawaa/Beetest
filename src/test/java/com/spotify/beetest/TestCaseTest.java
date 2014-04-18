/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spotify.beetest;

import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kawaa
 */
public class TestCaseTest {

    String resourcesDir = "src/main/resources";

    @Test
    public void testSetup() throws IOException, Exception {
        String query = new TestCase().getSetupQuery(
                "src/main/resources/tests/setup1.hql");
        assertEquals(query, "CREATE TABLE words(word STRING, length INT)\n"
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n"
                + "STORED AS TEXTFILE;\n");
    }

    @Test
    public void testShortSetup() throws IOException, Exception {
        String query = new TestCase().getShortSetupQuery(
                "src/main/resources/tests/ssetup1.hql");
        assertEquals(query, "CREATE TABLE words(word STRING, length INT)"
                + "\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';"
                + "\nLOAD DATA LOCAL INPATH 'input1.txt' INTO TABLE words;\n");
    }

    @Test
    public void test4() throws IOException, Exception {
        String expected = "DROP TABLE IF EXISTS words;\n"
                + "CREATE TABLE words(word STRING, length INT)\n"
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n"
                + "STORED AS TEXTFILE;\n"
                + "LOAD DATA LOCAL INPATH 'src/main/resources/tests/input1.txt' OVERWRITE INTO TABLE words;\n"
                + "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/beetest' \n"
                + "SELECT MAX(length) FROM words;";

        String[] args = {
            "-e", "src/main/resources/tests/output1.txt",
            "-s", "src/main/resources/tests/setup1.hql",
            "-q", "src/main/resources/tests/query1.hql",
            "-o", "/tmp/beetest",};
        String query = new TestCase().run(args);
        // assertEquals(expected, query);
    }
}
