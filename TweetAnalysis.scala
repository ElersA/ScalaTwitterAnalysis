
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
		
		// Read the positive and negative words which will be used in our sentiment analysis
		val posWords: Set[String] = readSentimentWords("./positive-words.txt")
		val negWords: Set[String] = readSentimentWords("./negative-words.txt")	

		// Assign credentials used by Oauth when creating the stream
		setupCredentials()

		// Use the streaming context and the TwitterUtils to create the Twitter stream.
		//val tweetDstream = TwitterUtils.createStream(ssc, None, Set("language=en", "")) // TODO add filter
		val tweetDstream = loadSentiment140File(sc, "./training.1600000.processed.noemoticon.csv")
		val tweetTexts = stream.flatMap(status => status.getText)

   		tweetTexts.foreachRDD { rdd =>
   			rdd.foreachPartition { partitionOfRecords =>
   				// Worker space

   				partitionOfRecords.foreach {tweet =>
   					// Perform sentiment analysis
   					sentimentAnalysis(tweet)

   					// Send result to Kafka topic

   				}

   			}
   		}	

		ssc.start()
		ssc.awaitTermination()
	}


	def loadSentiment140File(sc: SparkContext, sentiment140FilePath: String): DataFrame = {
	    val sqlContext = SQLContextSingleton.getInstance(sc)
	    val tweetsDF = sqlContext.read
	      .format("com.databricks.spark.csv")
	      .option("header", "false")
	      .option("inferSchema", "true")
	      .load(sentiment140FilePath)
	      .toDF("polarity", "id", "date", "query", "user", "status")

	    // Drop the columns we are not interested in.
	    tweetsDF.drop("polarity").drop("id").drop("date").drop("query").drop("user")
  	}

	def sentimentAnalysis(tweetText: String): Int = {
		
		// Split the tweet into words and process one word at a time
		val list = tweetText.split(" ");

        // Loop over words from stream and increase score.
        var score = 0
        for(word <- list){
            if (posWords.contains(word)) score++
            if (negWords.contains(word)) score--   
        }

        score match {
        	case s if s > 0 => 1	// Positive sentiment
        	case s if s < 0 => -1	// Negative sentiment
        	case s if s == 0 => 0	// Neutral sentiment	
        }
	}

	def readSentimentWords(path: String): Set[String] = Source.fromFile(path).getLines.toList.filterNot(_.contains(";")).tail.toSet

	def setupCredentials() = {
		// These lines set the system properties which twitter4j will use for authentication when using TwitterUtils.createStream(ssc, None)	
		val properties = Source.fromFile("./project.properties").getLines.toList
		
		val CONSUMERKEY = properties(0).split(" ")(2)
		val CONSUMERSECRET = properties(0).split(" ")(3)	
		val ACCESSTOKEN = properties(0).split(" ")(3)
		val ACCESSTOKENSECRET = properties(0).split(" ")(3)	

		System.setProperty("twitter4j.oauth.consumerKey", CONSUMERKEY)
		System.setProperty("twitter4j.oauth.consumerSecret", CONSUMERSECRET)
		System.setProperty("twitter4j.oauth.accessToken", ACCESSTOKEN)
		System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESSTOKENSECRET)
	}

}