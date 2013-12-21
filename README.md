Beetest
=======

Beetest is a super simple utility that helps you test your Apache Hive script without any Java knowledge.

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

When Beetest is passed a path to this directory, it will run setup.hql and query.hql locally on your machine, write output to a local directory and then compares it with expected output. If computed and expected output differ, then an exception is thrown.

Example
-----
Let's assume that we want to implement and test a query that calculates how many times a given artist was streamed at Spotify. Our input dataset is a tab-separated file that contains records with following schema:

	artist <tab> song <tab> user <tab> ts

A record means that a given song by a given artist was streamed by a given user at given time.

### Setup

In a setup script, 
* we delete an input table, if it already exists
* create an input table with an appropriate schema
* load sample data into an input table (this requires another file with sample data)

	$ cat artist_count/setup.hql
	DROP TABLE IF EXISTS streamed_songs;

	CREATE TABLE streamed_songs(artist STRING, song STRING, user STRING, ts TIMESTAMP)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
	STORED AS TEXTFILE;

	LOAD DATA LOCAL INPATH 'artist_count/input.tsv' INTO TABLE streamed_songs;

A file with sample data contains following content:

	$ cat artist_count/input.tsv
	Coldplay	Viva la vida	adam.kawa	2013-01-01 21:20:10
	Coldplay	Viva la vida	nat.stachura	2013-01-01 21:22:41
	Oasis	Wonderwall	adam.kawa	2013-01-02 02:33:55
	Coldplay	Yelllow	adam.kawa	2013-01-02 14:10:01
	Oasis	Wonderwall	dog.tofi	2013-01-02 22:17:51
	Aerosmith	Crazy	nat.stachura	2013-01-02 23:48:31

### Query

	$ cat artist_count/query.hql 
  	  SELECT artist, COUNT(*) AS cnt
    	    FROM streamed_songs
	GROUP BY artist
	ORDER BY cnt DESC
	   LIMIT 2;

### Expected output

	$ cat artist_count/expected 
	Coldplay^A3
	Oasis^A2

### Running a test

	$ ./run-test.sh artist_count config

Building project
-----
	https://github.com/kawaa/Beetest.git
	cd Beetest
	mvn3 -P full package

