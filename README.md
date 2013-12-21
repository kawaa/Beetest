Beetest
=======

Beetest is a super simple utility that helps you test your Hive script without any Java knowledge.

Motivation
----------
There are multiple reasons why Apache Hive was created by Facebook. One of the reasons is that many people, who wants to run distributed computation on top of Hadoop cluster, are excelent at SQL, but they do not know (or do not want to use) Java. Hive achieves this goal in 99% - although it provides a powerful SQL-like language called HiveQL, while UDFs, SerDes and unit tests must be still written in Java today.

In general, a Hive user is not a Java developer. While UDFs and SerDes can be implemented by colleagues who are fluent at Java, the unit test are expected to be written by Hive users (as they know the logic of their queries the best). If a process of testing Hive queries requires Java knowledge, then Hive users might have lower motivation to write them (and e.g. test queries by running them manually on the production cluster).

Idea
----------
Beetest allows you to define a unit test in a high-level way, and runs it locally on your machine. It requires Hive knowledge only.

To write a unit test, a user simply provides 3 files:

* query.hql - a script with a HiveQL query to test localy
* setup.hql - a script with setup instructions written in HiveQL (e.g. creating an input table and loading sample data into it)
* expected  - a file with expected output

and put them into a common directory.

When Beetest is passed a path to this directory, it will run setup.hql and query.hql locally on your machine, write output to a local directory and then compares it with expected output. If computed and expected output differ, then an exception is thrown.

Example
-----


Building project
-----
	https://github.com/kawaa/Beetest.git
	cd Beetest
	mvn3 -P full package


