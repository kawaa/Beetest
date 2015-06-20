package com.spotify.beetest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hive.service.server.HiveServer2;

@SuppressWarnings("deprecation")
public class MiniCluster {
	private static Configuration conf;
	private MiniDFSCluster hdfsCluster = null;
	private MiniMRCluster miniMR = null;
	private static HiveConf hiveConf;
	private HiveServer2 hiveServer2 = null;
	private int thriftBinaryPort;

	public MiniCluster(final String testDir, File inputConfig, String outDir,
			String outFileName) throws IOException, IllegalArgumentException,
			ConfigurationException {
		conf = new HdfsConfiguration();
		System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
		conf.clear();

		File baseDir = new File(testDir, "BeetestCluster");

		if (baseDir.exists()) {
			FileUtil.fullyDelete(baseDir);
		}

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		conf.set("dfs.replication", "1");

		// NOTE Maven Plugin breaks filesystem, so the following modification is
		// needed
		// http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("dfs.permissions.enabled", "false");
		System.setProperty("hadoop.log.dir", "${fs.default.name}" + "/logs"); // MAPREDUCE-2785
		conf.set("hive.root.logger", "ERROR,console");

		hiveConf = new HiveConf(conf,
				org.apache.hadoop.hive.ql.exec.CopyTask.class);

		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hiveConf);
		hdfsCluster = builder.build();

		// Create MR Cluster
		// FIXME Using MiniMRCluster is causing memory issues, revisit this
		// issue
		// int numTaskTrackers = 2;
		// miniMR = new MiniMRCluster(numTaskTrackers,
		// hdfsCluster.getFileSystem().getUri().toString(), 1);

		DistributedFileSystem dfs = hdfsCluster.getFileSystem();

		String beetestBaseDir = hdfsCluster.getURI().toString()
				+ "/tmp/beetest";
		String beetestWarehouseDir = beetestBaseDir + "/warehouse";
		String beetestMetastoreDir = testDir + "/metastore_db";
		String beetestLocalScratchDir = testDir + "/localScratchDir";
		String beetestScratchDir = beetestBaseDir + "/scratchdir";
		String connectionURL = "jdbc:derby:;databaseName="
				+ beetestMetastoreDir + ";create=true";

		// Create the required dirs in hdfs and set appropriate permissions
		dfs.mkdirs(new Path(beetestBaseDir));
		Utilities.createDirsWithPermission(hiveConf,
				new Path(beetestScratchDir), new FsPermission((short) 00733),
				true);
		Utilities.createDirsWithPermission(hiveConf, new Path(
				beetestWarehouseDir), new FsPermission((short) 00777), true);

		// FIXME Set the following System property when MiniMRCluster is fixed
		// System.setProperty("mapred.job.tracker", miniMR.createJobConf(new
		// JobConf(conf)).get("mapred.job.tracker"));
		System.setProperty(HiveConf.ConfVars.LOCALSCRATCHDIR.varname,
				beetestLocalScratchDir);
		System.setProperty(ConfVars.METASTORECONNECTURLKEY.varname,
				connectionURL);
		System.setProperty(ConfVars.SCRATCHDIR.varname, beetestScratchDir);

		hiveConf.setVar(ConfVars.LOCALSCRATCHDIR, beetestLocalScratchDir);
		hiveConf.setVar(ConfVars.METASTOREWAREHOUSE, beetestWarehouseDir);
		hiveConf.setVar(ConfVars.METASTORECONNECTURLKEY, connectionURL);
		hiveConf.setVar(ConfVars.SCRATCHDIR, beetestScratchDir);
		hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, "127.0.0.1");

		// Find a free port
		thriftBinaryPort = MetaStoreUtils.findFreePort();
		hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, thriftBinaryPort);
		hiveConf.setBoolVar(ConfVars.HIVE_CLI_PRINT_HEADER, false);
		hiveConf.setBoolVar(ConfVars.HIVE_START_CLEANUP_SCRATCHDIR, true);
		hiveConf.setBoolVar(ConfVars.HIVE_ERROR_ON_EMPTY_PARTITION, true);
		hiveConf.setBoolVar(ConfVars.HIVE_INSERT_INTO_EXTERNAL_TABLES, true);
		hiveConf.setBoolVar(ConfVars.EXECPARALLEL, true);
		hiveConf.setBoolVar(ConfVars.HIVECONVERTJOIN, true);
		hiveConf.setBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI, true);
		hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS,
				false); // https://issues.apache.org/jira/browse/HIVE-10294

		FileInputStream fis = new FileInputStream(inputConfig);
		hiveConf.addResource(fis);

		// Start the HiveServer2
		hiveServer2 = new HiveServer2();
		hiveServer2.init(hiveConf);
		hiveServer2.start();

		File tDir = new File(outDir);
		if (!tDir.exists()) {
			tDir.mkdir();
		}

		Writer writer = new FileWriter(outFileName);
		hiveConf.writeXml(writer);

		// FIXME Is there a better way to avoid the following FileNotFoundException?
		// Copy the extra JAR files provided in hive.aux.jars.path to hdfs
		// location
		// HiveServer2 is looking for the same location in hdfs and throwing
		// FileNotFoundException
		// Similar to the following:
		// http://mail-archives.apache.org/mod_mbox/hive-user/201306.mbox/%3CCAPbOs9u1g0vwXCkdgYAEvcgQ4gBwLrjqG6wmCjDS26-FhoJ56g@mail.gmail.com%3E
		if (hiveConf.get("hive.aux.jars.path") != null) {
			String auxJarPath = hiveConf.get("hive.aux.jars.path");
			auxJarPath = auxJarPath.replace("file:", "");

			
			// copy over the contents
			dfs.copyFromLocalFile(new Path(auxJarPath), new Path(hdfsCluster
					.getURI().toString() + auxJarPath));
		}

	}

	public HiveConf getConfig() {
		return hiveConf;
	}

	public MiniDFSCluster getCluster() {
		return hdfsCluster;
	}

	public int getThriftPort() {
		return thriftBinaryPort;
	}

	public void shutdownCluster() {
		if (miniMR != null) {
			miniMR.shutdown();
		}
		if (hdfsCluster != null) {
			hdfsCluster.shutdown();
		}
		if (hiveServer2 != null) {
			hiveServer2.stop();
		}
	}
}
