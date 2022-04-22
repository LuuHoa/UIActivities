name := "UiActivities"

version := "0.1"

scalaVersion := "2.11.0"

//resolvers += "Nexus" at "https://slflokydlnexs01.infoftps.com:8081/nexus/content/repositories/central"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided"

//// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
//libraryDependencies += "org.apache.spark" %% "spark-streaming_2.10" % "2.2.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-network-shuffle
libraryDependencies += "org.apache.spark" %% "spark-network-shuffle" % "2.2.0"