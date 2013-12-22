Beetest
=======

Beetest is a super simple utility for testing Apache Hive scripts locally for non-Java developers.
Beetest has been tested on Hadoop 2.2.0 (YARN) and Hive 0.12.0.

Motivation
----------
There are multiple reasons why Apache Hive was created by Facebook. One of the reasons is that many people, who wants to run distributed computation on top of Hadoop cluster, are excelent at SQL, but they do not know (or do not want to use) Java. Apache Hive achieves this goal in 99% - although it provides a powerful SQL-like language called HiveQL, while UDFs, SerDes and unit tests must be still written in Java today.

In general, a Hive user is not a Java developer. While UDFs and SerDes can be implemented by colleagues who are fluent at Java, the unit test are expected to be written by Hive users (as they know the logic of their queries the best). If a process of testing Hive queries requires Java knowledge, then Hive users might have lower motivation to write them (and e.g. test queries by running them manually on the production cluster).

Idea
----------
Beetest allows you to define a unit test in a high-level way, and runs it locally on your machine. It requires Apache Hive knowledge only.

To write a unit test, a user simply provides 3 files:

* query.hql - a script with a HiveQL query to test localy
* setup.hql - a script with setup instructions written in HiveQL (e.g. creating an input table and loading sample data into it)
* expected  - a file with expected output

and put them into a common directory.

When Beetest is passed a path to this directory, it will run setup.hql and query.hql scripts locally on your machine, write an output to a local directory and then compares it with expected output. If computed and expected output differ, then an exception is thrown.

Example: Two most popular artists
-----
Let's see Beetest in action!

Assume that we need to implement and test a Hive query that finds two the most frequently streamed artists at Spotify. Our input dataset is a tab-separated file that contains records with following schema:

	artist <tab> song <tab> user <tab> timestamp

A single record simply means that a given song by a given artist was streamed by a given user at given point of time.

According to the information in a previous section, we need to prepare a setup and query scripts and a file with expected output.

### Setup script

In a setup script,
* we delete an input table, if it already exists,
* create an input table with an appropriate schema,
* load sample data into an input table (this requires another file with sample data)

Having above in mind, our setup script might have a following content:

	$ cat artist-count/setup.hql
	DROP TABLE IF EXISTS streamed_songs;

	CREATE TABLE streamed_songs(artist STRING, song STRING, user STRING, ts TIMESTAMP)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
	STORED AS TEXTFILE;

	LOAD DATA LOCAL INPATH 'artist-count/input.tsv' INTO TABLE streamed_songs;

Since we use 'artist-count/input.tsv' file in our setup script, we need to create this file as well.

	$ cat artist-count/input.tsv
	Coldplay	Viva la vida	adam.kawa	2013-01-01 21:20:10
	Coldplay	Viva la vida	nat.stachura	2013-01-01 21:22:41
	Oasis	Wonderwall	adam.kawa	2013-01-02 02:33:55
	Coldplay	Yelllow	adam.kawa	2013-01-02 14:10:01
	Oasis	Wonderwall	dog.tofi	2013-01-02 22:17:51
	Aerosmith	Crazy	nat.stachura	2013-01-02 23:48:31

### Query

Since out input dataset is really small, it is very easy to produce a file with an expected output. Please not that Apache Hive uses Ctrl+A (^A) as a default separator for files (columns) in a text format.

	$ cat artist-count/query.hql 
  	  SELECT artist, COUNT(*) AS cnt
    	    FROM streamed_songs
	GROUP BY artist
	ORDER BY cnt DESC
	   LIMIT 2;

### Expected output

Please not that Apache Hive uses Ctrl+A (^A) as a default separator for files (columns) in a text format.

	$ cat artist-count/expected 
	Coldplay^A3
	Oasis^A2

### Test case directory

We put all files described above into "artist-count" directory:

	$ ls artist-count/
	expected  input.tsv  query.hql  setup.hql

### Running a test

We are very close to start testing our script! ;)

run-test.sh is a basic script that runs a test and verifies the output:

	$ cd src/examples
	$ ./run-test.sh <path-test-case-directory> <path-to-hive-config>

We run test with following parameters:

	$ ./run-test.sh artist-count local-config

Please note that Beetest's jar must be build before running a test. You can do it by typing a following command in a top-level directory:

	$ mvn3 -P full package

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

Building project
-----
	https://github.com/kawaa/Beetest.git
	cd Beetest
	mvn3 -P full package
