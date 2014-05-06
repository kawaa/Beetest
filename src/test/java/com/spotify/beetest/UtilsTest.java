/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spotify.beetest;

import java.io.IOException;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kawaa
 */
public class UtilsTest {

    private static final Logger LOGGER = Logger.getLogger(UtilsTest.class.getName());
    
    @Test
    public void testRunCommandLs() throws IOException, Exception {
        assertEquals(0, Utils.runCommand("ls src/main/resources/test/setup1.hql", LOGGER));
    }

    @Test
    public void testRunCommandDiff() throws IOException, Exception {
        assertEquals(1,
                Utils.runCommand("diff "
                + "src/main/resources/test/setup1.hql "
                + "src/main/resources/test/query1.hql", LOGGER));
    }
}