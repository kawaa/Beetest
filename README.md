Beetest
=======

Beetest is a simple utility for local testing of Apache Hive scripts for non-Java developers.
Beetest has been tested on Hadoop 2.2.0 (YARN) and Hive 0.12.0.

Motivation
----------
There are multiple reasons why Apache Hive was created by Facebook. One of the reasons is that many people, who wants to run distributed computation on top of Hadoop cluster, are excelent at SQL, but they do not know (or do not want to use) Java. Apache Hive achieves this goal in 99% - although it provides a powerful SQL-like language called HiveQL, while UDFs, SerDes and unit tests must be still written in Java today.

In general, a Hive user is not a Java developer. While UDFs and SerDes can be implemented by colleagues who are fluent at Java, the unit test are expected to be written by Hive users (as they know the logic of their queries the best). If a process of testing Hive queries requires Java knowledge, then Hive users might have lower motivation to write them (and e.g. test queries by running them manually on the production cluster).

### Idea

Please have a look at slides 31-52 from my presentation given at Hadoop Summit San Jose 2014 - http://www.slideshare.net/AdamKawa/a-perfect-hive-query-for-a-perfect-meeting-hadoop-summit-2014.

### Running a test

run-test.sh is a basic script that runs a test and verifies the output:

	$ git clone https://github.com/kawaa/Beetest.git
	$ cd Beetest
	$ mvn3 -P full package
	$ cd src/examples
	$ ./run-test.sh <path-test-case-directory> <path-to-hive-config>

We run test with following parameters:

	$ ./run-test.sh artist-count local-config

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
