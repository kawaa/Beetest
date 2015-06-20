package com.spotify.beetest;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.spotify.beetest.MiniCluster;
import com.spotify.beetest.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMiniCluster {
	private MiniCluster miniCluster;
	private static final Logger LOGGER = Logger.getLogger(UtilsTest.class
			.getName());
	@SuppressWarnings("unchecked")
	private static final String HADOOPDIR = StringUtils.join(
			"/tmp/MiniClusterTest-", Utils.getRandomPositiveNumber());

	private File inputConfig;
	private File testCase;
	private String outConfDir;
	private String outConfFileName;

	@Before
	public void setUp() throws IOException, IllegalArgumentException, ConfigurationException {
		LOGGER.log(Level.INFO, "STARTING CLUSTER at: " + HADOOPDIR);
		inputConfig = FileUtils.getFile("src/test/resources/local-config/hive-site2.xml");
		testCase = FileUtils.getFile("src/test/resources/artist-count");

		outConfDir = HADOOPDIR + "/MiniDFSClusterConfig/";
		outConfFileName = outConfDir + "local-config";

		miniCluster = null;
		miniCluster = new MiniCluster(HADOOPDIR, inputConfig, outConfDir,
				outConfFileName);
	}

	@Test
	public void testClusterCreation() throws IOException {
		URI uri = miniCluster.getCluster().getURI();
		LOGGER.log(Level.INFO, "URI: " + uri.toString());
		Assert.assertNotNull(uri.toString());
		Assert.assertTrue((new File(HADOOPDIR)).exists());
	}

	@Test
	public void testClusterConfiguration() throws IOException,
			ConfigurationException, InterruptedException {
		LOGGER.log(Level.INFO, "TESTING CONFIGURATION");

		Assert.assertTrue(inputConfig.isFile());
		Assert.assertTrue(testCase.isDirectory());
		Assert.assertTrue((new File(inputConfig.getAbsolutePath()).exists()));

		// Check if certain configuration fields are present
		Configuration conf = new Configuration();
		conf.addResource(new Path(outConfFileName));

		Assert.assertNotNull(conf.get("dfs.permissions.enabled"));
		Assert.assertEquals("false", conf.get("dfs.permissions.enabled"));
	}

	@Test
	public void testReloadConfig() {
		Configuration c = miniCluster.getConfig();
		Assert.assertNotSame(c.size(), 0);

		c.clear();
		Assert.assertEquals(c.size(), 0);

		c.reloadConfiguration();
		Assert.assertNotSame(c.size(), 0);
	}

	@After
	public void tearDown() throws IOException {
		LOGGER.log(Level.INFO, "STOPPING CLUSTER");
		if (miniCluster != null) {
			miniCluster.shutdownCluster();
		}

		File hadoopDir = new File(HADOOPDIR);

		if (hadoopDir.exists()) {
			FileUtils.deleteDirectory(hadoopDir);
		}
	}
}
