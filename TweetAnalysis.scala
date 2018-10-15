
// Needed for all Spark jobs.
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

// Only needed for Spark Streaming.
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._

// Only needed for utilities for streaming from Twitter.
import org.apache.spark.streaming.twitter._
//file read
import scala.io.Source


object TweetAnalysis {

	def main(args: Array[String]) {

		// Set up the Spark configuration with our app name and any other config
		// parameters you want (e.g., Kryo serialization or executor memory).
		val sparkConf = new SparkConf().setAppName("id2221prog").setMaster("local[2]")

		// Use the config to create a streaming context that creates a new RDD
		// with a batch interval of every 5 seconds.
		val ssc = new StreamingContext(sparkConf, Seconds(5))
		// Use the streaming context and the TwitterUtils to create the
		// Twitter stream.
		//val stream = TwitterUtils.createStream(ssc, None)

		val posWords = readSentimentWords("./positive-words.txt")
		val negWords = readSentimentWords("./negative-words.txt")


	}



	def readSentimentWords(path: String): Set[String] = val words = Source.fromFile(path).getLines.toList.filterNot(_.contains(";")).tail.toSet


}