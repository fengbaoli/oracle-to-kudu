Oracle Data To Kudu
==================
*This program uses Oracle JDBC to export Oracle table data to CSV files, then upload CSV files to HDFS, and finally use impala JDBC to operate kudu, build tables and import data*

Get Start
-----------------------------
#### 1.clone src to load path 
git clone  https://github.com/fengbaoli/oracle-to-kudu.git
#### 2.download impala jdbc ,oracle jdbc and configure local maven repertory
##### <1>This example shows how to build and run a Maven-based project to execute SQL queries on Impala using JDBC #####
*This example was tested using Impala 2.3 included with CDH 5.5.2 and the Impala JDBC Driver v2.5.30*

When you download the Impala JDBC Driver from the link above, it is packaged as a zip file with separate distributions for JDBC3, JDBC4
and JDBC4.1. This example uses the distribution for JDBC4.1 on RHEL6 x86_64. The downloaded zip file contains the following eleven jar files:
###### (1)  ImpalaJDBC41.jar 
###### (2)  TCLIServiceClient.jar 
###### (3)  hive_metastore.jar 
###### (4)  hive_service.jar 
###### (5)  ql.jar 
###### (6)  libfb303-0.9.0.jar 
###### (7)  libthrift-0.9.0.jar 
###### (8)  log4j-1.2.14.jar 
###### (9)  slf4j-api-1.5.11.jar 
###### (10) slf4j-log4j12-1.5.11.jar 
###### (11) zookeeper-3.4.6.jar 

Manually configure project dependency packages using the MVN command
for example configure zookeeper-3.4.6.jar maven：
mvn install:install-file -Dfile=zookeeper-3.4.6.jar -DgroupId=ora.apache.zookeeper -DartifactId=zookeeper -Dversion=3.4.6  -Dpackaging=jar
##### <2>Download JDBC from Oracle's official website and configure JDBC manually
mvn install:install-file -Dfile=ojdbc6.jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=6.0  -Dpackaging=jar 
#### 3.build
mvn package

#### 4 deploy
##### <1>create dir and put oracle-kudu-1.0-SNAPSHOT.jar into it 
##### <2>Create the conf, data, and logs directories in the jar package sibling directory 
##### <3>copy edk.properties ,log4j.properties,pk.properties into conf dir 
##### <4>modify configure 
###### (1)edk.properties
ora_url=jdbc:oracle:thin:@10.205.44.53:1521:ora11g   *oracle jdbc url*
ora_username = test *oracle export username*
ora_password = test *oracle export password*

impala_url=jdbc:impala://hadoop4:21050 *impala jdnc url*
impala_database = oracle *impala database name*

batch_size=4 *per export tables nums*
hdfssuperuser = hdfs *hdfs supper username*
fs.defaultFS=hdfs://ns1 *hdfs ha*
dfs.nameservices=ns1 *hdfs ha nameservices*

#local_path=C:\\Users\\Administrator\\Desktop\\export-data-kudu\\data
local_path=data *local export path,default data dir*
hdfs_path=/opt/ogg *hdfs unload path*


#timezone=Asia/Shanghai
timezone = PRC *csv file timezone transfer*
#??????
skip_tables= *no export tablename,ie:tesst1,test2*
###### (2)pk.properties 
###### (3)log4j.properties 




