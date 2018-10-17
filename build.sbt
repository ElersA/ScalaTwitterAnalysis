name := "spark_kafka"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.1",
)



	/*
      "org.twitter4j" % "twitter4j-core" % "4.0.7",
      "org.twitter4j" % "twitter4j-stream" % "4.0.7"
  */

 

