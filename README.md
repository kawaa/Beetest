Beetest
=======

Beetest is a simple utility for testing of Apache Hive scripts locally for non-Java developers.
Beetest has been tested on Hadoop 2.2.0 (YARN) and Hive 1.2.1.

Motivation
----------
There are multiple reasons why Apache Hive was created by Facebook. One of the reasons is that many people, who want to run distributed computation on top of Hadoop cluster, are excelent at SQL, but they do not know (or do not want to use) Java. Apache Hive achieves this goal in 99% - although it provides a powerful SQL-like language called HiveQL, there is still number of useful features that must be implement in Java e.g. UDFs, SerDes and unit tests - to name a few.

In general, a Hive user is not necessarily a Java developer. While UDFs and SerDes can be implemented by colleagues who are fluent at Java, the unit test are expected to be written by Hive users (as they know the logic of their queries the best). If a process of testing Hive queries is tedious and requires Java skills, then Hive users might have lower motivation to write them (and, in a result, test queries by running them manually on the production cluster).

### Idea

Please have a look at slides 31-52 from my presentation given at Hadoop Summit San Jose 2014 - http://www.slideshare.net/AdamKawa/a-perfect-hive-query-for-a-perfect-meeting-hadoop-summit-2014.

### Example

run-test.sh is a basic script that runs a test and verifies the output:

	$ git clone https://github.com/kawaa/Beetest.git
	$ cd Beetest
	$ mvn -P full package     # requires Maven 3.X
	$ cd src/examples
	$ ./run-test.sh <path-test-case-directory> <path-to-hive-config>

We run test with following parameters:

The set of arguments needed are:
* Directory with set of query, property and expected result files
* Directory containing local hive config
* (optional -- defaulted to FALSE) TRUE/FALSE to create a MiniDFSCluster and run the queries via HiveServer2
	* Without MiniDFSCluster (assuming that the env is setup with Hive and Hadoop)
		* In the following execution, MiniDFSCluster is not created and the test directory deletion is defaulted to TRUE, hence, deleted on the completion of the test.
			$ ./run-test.sh artist-count-ddl local-config	
		  	
		* In the following execution, MiniDFSCluster is not created and the state of the test directory can be preserved at the end of the test as needed.
			$ ./run-test.sh artist-count-ddl local-config FALSE <TRUE/FALSE>
		  
		
	* With MiniDFSCluster
		* In the following execution, MiniDFSCluster is created and the test directory deletion is defaulted to TRUE, hence, deleted on the completion of the test.
			$ ./run-test-locally.sh artist-count-ddl local-config TRUE
		  
		* In the following execution, MiniDFSCluster is created and the state of the test directory can be preserved at the end of the test as needed. 
			$ ./run-test-locally.sh artist-count-ddl local-config TRUE <TRUE/FALSE>
* (optional -- defaulted to TRUE) TRUE/FALSE to delete the Beetest directory.
		  

### How it works

A unit test is represented as a directory that consists of several files
* `select.hql` - a query to test

The query below find two artists with the largest number of streams:

	SELECT artist, COUNT(*) AS cnt
	FROM ${table}
	GROUP BY artist
	ORDER BY cnt DESC
	LIMIT 2;

* `table.ddl` - schemas of input tables

The input table has a following schema:

	stream(artist STRING, song STRING, user STRING, ts TIMESTAMP)

* text files with input data.

These files should be named in the same way as tables e.g. `stream.txt` contains input records for the `stream` table

	Coldplay	Viva la vida	adam.kawa	2013-01-01 21:20:10
	Coldplay	Viva la vida	natalia.stachura	2013-01-01 21:22:41
	Oasis	Wonderwall	adam.kawa	2013-01-02 02:33:55
	Coldplay	Yelllow	adam.kawa	2013-01-02 14:10:01
	Oasis	Wonderwall	dog.tofi	2013-01-02 22:17:51
	Aerosmith	Crazy	natalia.stachura	2013-01-02 23:48:31

* `expected.txt` - expected output

The expected (and right) answer is as follows:

	Coldplay	3
	Oasis	2

* (optional) `setup.hql` - any initialization query Beetest e.g. setting values of configuration settings

Once you run the `run-test.sh` file, BeeTest will use these files to generate and execute Hive script that, in local mode, will create necessary input tables, load input data into them, execute your query and verify if it returns expected output.

	./run-test.sh artist-count-ddl local-config

	Dec 14, 2014 2:18:29 PM com.spotify.beetest.TestQueryExecutor run
	INFO: Generated query filename: /tmp/beetest-test-310486961-query.hql
	Dec 14, 2014 2:18:29 PM com.spotify.beetest.TestQueryExecutor run
	INFO: Generated query content:

	CREATE DATABASE IF NOT EXISTS beetest;
	USE beetest;
	DROP TABLE IF EXISTS stream;
	CREATE TABLE stream(artist STRING, song STRING, user STRING, ts TIMESTAMP)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
	LOAD DATA LOCAL INPATH 'artist-count-ddl/stream.txt' INTO TABLE stream;
	DROP TABLE IF EXISTS output_310486961;
	CREATE TABLE output_310486961
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
	LOCATION '/tmp/beetest-test-310486961-output_310486961' AS

  	SELECT artist, COUNT(*) AS cnt
    	FROM stream
	GROUP BY artist
	ORDER BY cnt DESC
	LIMIT 2;

	...

	Dec 14, 2014 2:18:49 PM com.spotify.beetest.Utils runCommand
	INFO: Table beetest.output_310486961 stats: [numFiles=0, numRows=2, totalSize=0, rawDataSize=17]
	Dec 14, 2014 2:18:49 PM com.spotify.beetest.Utils runCommand
	INFO: OK
	Dec 14, 2014 2:18:49 PM com.spotify.beetest.Utils runCommand
	INFO: Time taken: 12.596 seconds

	Dec 14, 2014 2:18:49 PM com.spotify.beetest.TestQueryExecutor run
	INFO: Asserting: artist-count-ddl/expected.txt and /tmp/beetest-test-310486961-output_310486961/000000_0

At the end, no exception/error is thrown! This means that the test passes! And the test took only 12 seconds! (comparing to minutes, if we want to test it on cluster!).
You can also review the output generated by your query by opening the output file - in this case /tmp/beetest-test-310486961-output_310486961/000000_0.

	$ cat /tmp/beetest-test-310486961-output_310486961/000000_0
	Coldplay	3
	Oasis	2

Test isolation
-----

Beetest will create own database (if it already does not exists) called "beetest" and create all tables there. This have two advantages:
* we can use the same name of tables as in the production system
* if we drop (accidentally or not) a table during unit testing, a testing table will be dropped and production tables will be untouched.

Local configuration
-----
We run a test locally, because we override a couple of Hive settings:

	$ cat local-config/hive-site.xml

	<property>
		<name>beetest.dir</name>
		<value>/tmp/beetest/${user.name}</value>
	</property>
	<property>
		<name>fs.default.name</name>
		<value>file://${beetest.dir}</value>
	</property>
	<property>
		<name>hive.metastore.warehouse.dir</name>
		<value>file://${beetest.dir}/warehouse</value>
	</property>
	<property>
		<name>javax.jdo.option.ConnectionURL</name>
		<value>jdbc:derby:;databaseName=${beetest.dir}/metastore_db;create=true</value>
	</property>
	<property>
		<name>javax.jdo.option.ConnectionDriverName</name>
		<value>org.apache.derby.jdbc.EmbeddedDriver</value>
	</property>
	<property>
		<name>mapreduce.framework.name</name>
		<value>local</value>
	</property>
	
Adding Custom UDF's in the Hive Queries:
-----
In order to test custom UDF's in Hive in the MiniDFSCluster via HiveServer2:

	* Update hive-site.xml to provide the following:
	
		<property>
  		  <name>hive.aux.jars.path</name>
  		  <value>file:///tmp/beetest-lib</value>
		</property>
		 
	* Make sure the folder exists on local file system
	* If the above property is set, MiniDFSCluster will copy over the jars into hdfs and use it for testing UDF's

License
-----
Beetest is released under the Apache License Version 2.0.
