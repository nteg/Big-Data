# Big-Data
This directory will contain the Apache Hadoop POC projects.


Commands to make jar: (jar is also checked-in 'dist' folder)

	// For setting HADOOP_CLASSPATH
	export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

	// Compiling or building all the files
	hadoop com.sun.tools.javac.Main -d classes/ YammerMapper.java
	hadoop com.sun.tools.javac.Main -d classes/ YammerReducer.java
	hadoop com.sun.tools.javac.Main -d classes/ YammerAnalysis.java

	// Creating jar with name YammerAnalysis.jar
	jar -cvf YammerAnalysis.jar -C classes/ . 
	
	
Commands to run the Hadoop task:

	// Running Hadoop Task:
	hadoop jar <absolute path of jar file>/YammerAnalysis.jar com.yammer.analysis.YammerAnalysis <input directory in hdfs> <output directory in hdfs(must not exist)>
	// For instance
	hadoop jar /home/namenode/Desktop/YammerAnalysisPOC/YammerAnalysis.jar com.yammer.analysis.YammerAnalysis /inputData/#5 /outputData/YammerAnalysis/ApplicationOutput