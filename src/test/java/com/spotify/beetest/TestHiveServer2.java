package com.spotify.beetest;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.logging.Logger;
import java.util.logging.Level;

import junit.framework.Assert;

import com.spotify.beetest.MiniCluster;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHiveServer2 {
	private MiniCluster miniCluster;
	private static final Logger LOGGER = Logger.getLogger(UtilsTest.class
			.getName());

	private File inputConfig;
	private File testCase;
	private String outConfDir;
	private String outConfFileName;
	private TestCase tc;

	@Before
	public void setUp() throws IOException, IllegalArgumentException, ConfigurationException {
		inputConfig = FileUtils.getFile("src/test/resources/local-config/hive-site2.xml");
		testCase = FileUtils.getFile("src/test/resources/artist-count");

		miniCluster = null;
		tc = null;

		tc = new TestCase(testCase.getAbsolutePath());
		outConfDir = tc.getTestDir() + "/MiniDFSClusterConfig/";
		outConfFileName = outConfDir + "local-config";
		miniCluster = new MiniCluster(tc.getTestDir(), inputConfig, outConfDir,
				outConfFileName);

	}

	@Test
	public void testHiveQuery() {
		try {
			int querySuccess = TestQueryExecutor.run(tc, outConfFileName, miniCluster);
			Assert.assertEquals(0, querySuccess);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void printResult(ResultSet rs) {
		ResultSetMetaData rsmd;
		int numberOfColumns;
		try {
			rsmd = rs.getMetaData();
			numberOfColumns = rsmd.getColumnCount();

			System.out.println("\n=======================PRINTING RESULT========================\n");
			while (rs.next()) {
				for (int i = 1; i <= numberOfColumns; i++) {
					if (i > 1)
						System.out.print(",  ");
					String columnValue = rs.getString(i);
					System.out.print(columnValue);
				}
				System.out.println("");
			}
			System.out.println("\n==============================================================\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@After
	public void tearDown() throws IOException {
		LOGGER.log(Level.INFO, "STOPPING CLUSTER");
		if (miniCluster != null) {
			miniCluster.shutdownCluster();
		}

		if(tc != null) {
			TestQueryExecutor.deleteTestDir(true, tc.getTestDir(), tc.getTestCaseQueryFilename());
		}
	}
}