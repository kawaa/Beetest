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
    public void testShortSetup() throws IOException, Exception {
        String query = new TestCase().getDDLSetupQuery(
                "src/main/resources/tests/ssetup1.hql");
        assertEquals(query, "DROP TABLE IF EXISTS words;"
                + "\nCREATE TABLE words(word STRING, length INT)"
                + "\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';"
                + "\nLOAD DATA LOCAL INPATH 'input1.txt' INTO TABLE words;\n");
    }
}
