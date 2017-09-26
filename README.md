oracle dat to kudu
==================
This program uses Oracle JDBC to export Oracle table data to CSV files, then upload CSV files to HDFS, and finally use impala JDBC to operate kudu, build tables and import data

get start
===================
### 1.clone src to load path<br />
-----------------------
git clone ### https://github.com/fengbaoli/oracle-to-kudu.git
### 2.download impala jdbc ,oracle jdbc and configure local maven repertory<br />
-----------------------
### <1>This example shows how to build and run a Maven-based project to execute SQL queries on Impala using JDBC 
This example was tested using Impala 2.3 included with CDH 5.5.2 and the Impala JDBC Driver v2.5.30 

When you download the Impala JDBC Driver from the link above, it is packaged as a zip file with separate distributions for JDBC3, JDBC4
and JDBC4.1. This example uses the distribution for JDBC4.1 on RHEL6 x86_64. The downloaded zip file contains the following eleven jar files:
##### (1)  ImpalaJDBC41.jar 
##### (2)  TCLIServiceClient.jar
##### (3)  hive_metastore.jar 
##### (4)  hive_service.jar 
(5)  ql.jar\<br> 
(6)  libfb303-0.9.0.jar\<br> 
(7)  libthrift-0.9.0.jar\<br> 
(8)  log4j-1.2.14.jar\<br> 
(9)  slf4j-api-1.5.11.jar\<br> 
(10) slf4j-log4j12-1.5.11.jar\<br> 
(11) zookeeper-3.4.6.jar\<br> 

Manually configure project dependency packages using the MVN command\<br> 
for example configure zookeeper-3.4.6.jar maven：\<br> 
mvn install:install-file -Dfile=zookeeper-3.4.6.jar -DgroupId=ora.apache.zookeeper -DartifactId=zookeeper -Dversion=3.4.6  -Dpackaging=jar\<br> 
<2>Download JDBC from Oracle's official website and configure JDBC manually\<br> 
mvn install:install-file -Dfile=ojdbc6.jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=6.0  -Dpackaging=jar\<br> 
### 3.build<br />
---------------------------------
mvn package

### 4 deploy<br />
------------------------------------
<1>create dir and put oracle-kudu-1.0-SNAPSHOT.jar into it\<br> 
<2>Create the conf, data, and logs directories in the jar package sibling directory\<br> 
<3>copy edk.properties ,log4j.properties,pk.properties into conf dir\<br> 
<4>modify configure\<br> 




