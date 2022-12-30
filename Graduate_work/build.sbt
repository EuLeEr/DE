name := "Graduate Project"
//version := "1.0"
//scalaVersion := "2.13"
//libraryDependencies += "org.apache.spark" %% "spark-sql3.3.1"
version := "1.0"
scalaVersion := "2.13.10"

val sparkVersion = "3.3.1"

// Note the dependencies are provided
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion 
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion 
