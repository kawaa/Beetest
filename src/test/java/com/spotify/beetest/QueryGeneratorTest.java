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
public class QueryGeneratorTest {

    String resourcesDir = "src/main/resources";

    @Test
    public void test1() throws IOException, Exception {
        String query = new QueryGenerator().getSetupQuery(
                "src/main/resources/tests/setup1.hql");
    }

    @Test
    public void test2() throws IOException, Exception {
        String query = new QueryGenerator().getTestedQuery("output",
                "src/main/resources/tests/query1.hql");
    }

    @Test
    public void test3() throws IOException, Exception {
        String expected = "DROP TABLE IF EXISTS words;\n"
                + "CREATE TABLE words(word STRING, length INT)\n"
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n"
                + "STORED AS TEXTFILE;\n"
                + "LOAD DATA LOCAL INPATH 'src/main/resources/tests/input1.txt' OVERWRITE INTO TABLE words;\n"
                + "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/beetest' \n"
                + "SELECT MAX(length) FROM words;";
        
        String[] args = {"-t", "words",
            "-i", "src/main/resources/tests/input1.txt",
            "-e", "src/main/resources/tests/output1.txt",
            "-s", "src/main/resources/tests/setup1.hql",
            "-q", "src/main/resources/tests/query1.hql",
            "-o", "/tmp/beetest",};
        String query = new QueryGenerator().run(args);
        // assertEquals(expected, query);
    }
}
