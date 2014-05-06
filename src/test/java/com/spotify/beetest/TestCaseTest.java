/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spotify.beetest;

import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author kawaa
 */
public class TestCaseTest {

    String resourcesDir = "src/main/resources";

    @Before
    public void initialize() throws Exception {
    }

    @Test
    public void testOneInputFile() throws IOException, Exception {
        String query = new TestCase().getDDLSetupQuery(
                "src/main/resources/test/table.ddl", "test");

        String expected = "DROP TABLE IF EXISTS words;"
                + "\nCREATE TABLE words(word STRING, length INT)"
                + "\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';"
                + "\nLOAD DATA LOCAL INPATH 'input1.txt' INTO TABLE words;\n";

        assertEquals(query, expected);
    }

    @Test
    public void testDefaultInputFile() throws IOException, Exception {
        String query = new TestCase().getDDLSetupQuery(
                "src/main/resources/test/table-default-file.ddl", "test");

        String expected = "DROP TABLE IF EXISTS words;"
                + "\nCREATE TABLE words(word STRING, length INT)"
                + "\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';"
                + "\nLOAD DATA LOCAL INPATH 'test/words.txt' INTO TABLE words;\n";

        assertEquals(query, expected);
    }

    @Test
    public void testTwoInputFiles() throws IOException, Exception {
        String query = new TestCase().getDDLSetupQuery(
                "src/main/resources/test/table-two-files.ddl", "test");

        String expected = "DROP TABLE IF EXISTS words;\n"
                + "CREATE TABLE words(word STRING, length INT)\n"
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';\n"
                + "LOAD DATA LOCAL INPATH 'input1.txt' INTO TABLE words;\n"
                + "LOAD DATA LOCAL INPATH 'input2.txt' INTO TABLE words;\n";

        assertEquals(query, expected);
    }

    @Test
    public void testNoInputFile() throws IOException, Exception {
        String query = new TestCase().getDDLSetupQuery(
                "src/main/resources/test/table-no-file.ddl", "test");

        String expected = "DROP TABLE IF EXISTS words;"
                + "\nCREATE TABLE words(word STRING, length INT)"
                + "\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';\n";

        assertEquals(query, expected);
    }
}
