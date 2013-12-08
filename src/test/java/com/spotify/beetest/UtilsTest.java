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
public class UtilsTest {

    @Test
    public void testRunCommandLs() throws IOException, Exception {
        assertEquals(0,
                Utils.runCommand("ls src/main/resources/tests/setup1.hql"));
    }

    @Test
    public void testRunCommandDiff() throws IOException, Exception {
        assertEquals(1,
                Utils.runCommand("diff "
                + "src/main/resources/tests/setup1.hql "
                + "src/main/resources/tests/query1.hql"));
    }
}