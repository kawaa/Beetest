package com.spotify.beetest;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.lang.IllegalArgumentException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.commons.io.FileUtils;

import com.spotify.beetest.TestQueryExecutor;
import com.spotify.beetest.MiniCluster;

/**
 *
 * @author kawaa
 */
public class TestQueryExecutor {

    private static final Logger LOGGER =
            Logger.getLogger(TestQueryExecutor.class.getName());
    private static final Map<String, String> variableMap = new HashMap<String, String>();

    private static String getTestCaseCommand(String config, String queryFilename, Properties variables) {
    	LOGGER.log(Level.INFO, "CONFIG BEING USED IS: " + config);
        List<String> args = new ArrayList<String>(Arrays.asList("hive", "--config", config, "-f", queryFilename));
        for (String key :  variables.stringPropertyNames()) {
            args.add("--hivevar");
            args.add(key + "=" + variables.get(key));
            variableMap.put(key, (String) variables.get(key));
        }
        return StringUtils.join(args, " ");
    }

    public static int run(TestCase tc, String config, MiniCluster miniCluster) throws IOException, InterruptedException {
        String queryFilename = tc.generateTestCaseQueryFile();

        LOGGER.log(Level.INFO, "Generated query filename: {0}", queryFilename);
        LOGGER.log(Level.INFO, "Generated query content: \n{0}", tc.getBeeTestQuery());

        Properties variables = new Properties();
        try {
            variables.load(new FileInputStream(tc.getVariablesFilename()));
        } catch (IOException ex) {
            LOGGER.log(Level.INFO, "Missing variables file");
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.SEVERE, "Invalid variables file");
        }

        // variableMap is being populated in getTestCaseCommand, so, 
        // using this call indirectly in the case of minicluster
        String testCaseCommand = getTestCaseCommand(config, queryFilename, variables);
        
        String outFileName = "";
        if(miniCluster != null) {
        	executeHiveQuery(tc, miniCluster);
			
        	DistributedFileSystem dfs = miniCluster.getCluster().getFileSystem();
        	
			try {
				Path dfsPath = new Path(dfs.getUri().toString()+"/");
	        	
	        	if(dfs.exists(dfsPath)) {
	        		File outDir = new File(tc.getTestDir() + "/outputDirs"); 
		        	if(!outDir.exists()) {
		        		outDir.mkdirs();
		        	}
	        		Utils.copyToLocalDir(dfs, dfsPath, outDir, miniCluster.getConfig());
	        	}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			outFileName = tc.getTestDir() + "/outputDirs/000000_0";
        } else {
            LOGGER.log(Level.INFO, "Running: {0}", testCaseCommand);
        	Utils.runCommand(testCaseCommand, LOGGER);
            outFileName = tc.getOutputFilename();
        }
        LOGGER.log(Level.INFO, "Asserting: {0} and {1}",
                new Object[]{tc.getExpectedFilename(), outFileName});
		
		if (FileUtils.contentEquals(new File(tc.getExpectedFilename()),
				new File(outFileName))) {
			return 0;
		} else {
			LOGGER.log(
					Level.SEVERE,
					"Output does not match. Contents of "
							+ tc.getExpectedFilename() + " != " + outFileName);
			return 1;
		}
    }
    
    public static void deletePath(File tDir) {
    	if(tDir.isFile()) {
    		tDir.delete();
    	} else {
			File[] list = tDir.listFiles();
			if (list != null) {
				for (File listEle : list) {
					deletePath(listEle);
				}
			}
			tDir.delete();
    	}
    }
    
    public static void deleteTestDir(boolean deleteTestDirOnExit, String testDir, String queryFileName) {
    	if (deleteTestDirOnExit) {
        	File tDir = new File(testDir);
        	if(tDir.exists()) {
        		deletePath(tDir);
        	}
        	
        	// delete formulated query file
        	File queryFile = new File(queryFileName);
        	if (queryFile.exists()) {
        		deletePath(queryFile);
        	}
        }
    }
    
    public static void executeHiveQuery(TestCase tc, MiniCluster miniCluster) {
    	String driverName = "org.apache.hive.jdbc.HiveDriver";
		try {
			Class.forName(driverName);
			
			StrSubstitutor substitutor = new StrSubstitutor(variableMap);
			CommandProcessor cp = null;
			
			ArrayList<String> queryList = getQueryList(substitutor,
					tc.getBeeTestQuery());

			for (String query : queryList) {
				query = query.replaceAll("\n", " ");
				if (query.trim().length() > 0) {
					cp = CommandProcessorFactory.getForHiveCommand(query.trim()
							.split("\\s+"), miniCluster.getConfig());

					if (cp == null) {
						cp = new Driver(miniCluster.getConfig());
					}

					// FIXME When Apache Hive resolves this issue, remove the
					// following hack
					// NOTE Handling special case for
					// https://issues.apache.org/jira/browse/HIVE-6971
					String qq = "";
					if (cp instanceof org.apache.hadoop.hive.ql.processors.AddResourceProcessor) {
						// Not doing a case insensitive search here by converting to lowercase
						// that will convert the rest of the string to lowercase which is not
						// desirable
						if(query.contains("add ")) {
							qq = query.split("add ")[1].trim();
						} else if(query.contains("ADD ")) {
							qq = query.split("ADD ")[1].trim();
						}
					} else {
						qq = query.trim();
					}
					cp.run(qq.trim());
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    public static ArrayList<String> getQueryList(StrSubstitutor substitutor, String fullQuery) {
    	ArrayList<String> li = new ArrayList<String>();
    	
    	for (String query: fullQuery.split(";")) {
    		li.add(substitutor.replace(query));
    	}
    	
    	return li;
    }
    
    public static void main(String[] args)
            throws ParseException, IOException, InterruptedException, ConfigurationException {
    	MiniCluster miniCluster = null;
    	Boolean createMiniCluster;
    	Boolean deleteBeetestTestDirOnExit = true;
    	TestCase tc = null;
    	int querySuccess = 0;
    	
    	try {
    		if (args.length < 2) {
    			System.err.println("Number of command line arguments should be atleast 2 \n");
    			System.exit(1);
    		}
    		
			String testCase = args[0];
			String config = args[1];
			
    		if(args.length == 2) {
    			createMiniCluster = false;
    			deleteBeetestTestDirOnExit = true;
    		} else if (args.length == 3) {
    			createMiniCluster = Boolean.parseBoolean(args[2]);
    			deleteBeetestTestDirOnExit = true;
    		} else {
    			createMiniCluster = Boolean.parseBoolean(args[2]);
    			deleteBeetestTestDirOnExit = Boolean.parseBoolean(args[3]);
    		}
    		
	    	tc = new TestCase(testCase);
	    	String outConfDir = tc.getTestDir() + "/MiniDFSClusterConfig/";
	    	String outConfFileName = outConfDir + "local-config";
	    	String conf;
	    	
	    	if(createMiniCluster) {
	    		// Start MiniDFSCluster
	        	miniCluster = new MiniCluster(tc.getTestDir(), new File(config), outConfDir, outConfFileName);
	        	conf = outConfFileName;
	    	} else {
	    		conf = config;
	    	}
	        
	    	querySuccess = run(tc, conf, miniCluster);
	    	
	    	if(miniCluster != null) {
	    		miniCluster.shutdownCluster();
	    	}
    	} catch (Exception e) {
    		e.printStackTrace();
    		if(miniCluster != null) {
	    		miniCluster.shutdownCluster();
	    	}
    	}
    	
    	// Cleanup
    	if (tc != null) {
    		deleteTestDir(deleteBeetestTestDirOnExit, tc.getTestDir(), tc.getTestCaseQueryFilename());
    	}
    	
    	System.exit(querySuccess);
    }
}
