/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spotify.beetest;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author kawaa
 */
public class Utils {

    private static Random random = new Random();

    public static int getRandomPositiveNumber() {
        return (random.nextInt() & Integer.MAX_VALUE);
    }

    public static int runCommand(String command, Logger LOGGER)
            throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();

        BufferedReader reader =
                new BufferedReader(new InputStreamReader(p.getErrorStream()));

        String line = reader.readLine();
        while (line != null) {
            LOGGER.info(line);
            line = reader.readLine();
        }

        return p.exitValue();
    }

    public static String readFile(String pathname) throws IOException {
        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(file);
        String lineSeparator = System.getProperty("line.separator");

        try {
            while (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine());
                fileContents.append(lineSeparator);
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

    public static List<String> fileToList(String pathname) throws IOException {
        File file = new File(pathname);
        Scanner scanner = new Scanner(file);
        List<String> list = new ArrayList<String>();
        try {
            while (scanner.hasNextLine()) {
                list.add(scanner.nextLine());
            }
            return list;
        } finally {
            scanner.close();
        }
    }

    public static boolean deletePath(String filename) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        return fs.delete(new Path(filename), true);
    }
}
