
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

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import kafka.producer.KeyedMessage


object TweetAnalysis {

	def main(args: Array[String]) {

		// Set up the Spark configuration with our app name and any other config
		// parameters you want (e.g., Kryo serialization or executor memory).
		val sparkConf = new SparkConf().setAppName("id2221prog").setMaster("local[2]")

		// Use the config to create a streaming context that creates a new RDD
		// with a batch interval of every 5 seconds.
		val ssc = new StreamingContext(sparkConf, Seconds(10))	

    	

		// Assign credentials used by Oauth when creating the stream
		setupCredentials()

		// Setup KafkaProducer
	  	val topic = "twitter"
    	val brokers = "localhost:9092"
    	val props = new Properties()
	    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
	    props.put(ProducerConfig.CLIENT_ID_CONFIG, "TweetProducer")
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
	    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
		
		// Use the streaming context and the TwitterUtils to create the Twitter stream. The last argument is our filter.
		//val tweetDstream = TwitterUtils.createStream(ssc, None, Seq("realDonaldTrump", "notMyPresident"))
		val tweetDstream = TwitterUtils.createStream(ssc, None,Seq("realDonaldTrump", "notMyPresident"))

		// Get all tweets that are in english and then only save the text of each tweet
		val tweetTexts = tweetDstream.filter(x=> x.getLang() == "en" && !(x.getText.startsWith("@")) && x.getText.length>1).map(x=> (sentimentAnalysis(x.getText),x.getText))


		tweetTexts.foreachRDD { rdd =>
			rdd.foreachPartition { partitionOfRecords =>
				val producer = new KafkaProducer[String, String](props)
				partitionOfRecords.foreach{record => 
					val data = new ProducerRecord[String, String](topic, record._1.toString , record._2)
					producer.send(data)
					}
				producer.close()			
			}
		}
		/*
   		tweetTexts.foreachRDD { rdd =>
   			// Master space
   			rdd.foreachPartition { partitionOfRecords =>
   				// Worker space
   				//val producer = new KafkaProducer[String, String](props)
   				partitionOfRecords.foreach {tweet =>
					//val sentiment = sentimentAnalysis(tweet) // Perform sentiment analysis

   					// Send result to Kafka topic
			        val data = new ProducerRecord[String, String](topic, sentiment.toString, tweet)
			        producer.send(data)
				
				producer.close()			
			}
		}
		*/
		ssc.start()
		ssc.awaitTermination()
	}

	// Read the positive and negative words which will be used in our sentiment analysis
	val posWords: Set[String] = readSentimentWords("./positive-words.txt")
	val negWords: Set[String] = readSentimentWords("./negative-words.txt")	

	def sentimentAnalysis(tweetText: String): Int = {
		
		// Split the tweet into words and process one word at a time
		val list = tweetText.split(" ");

        // Loop over words from stream and increase score.
        var score = 0
        for(word <- list){
            if (posWords.contains(word.toLowerCase())) {
            	score += 1
            }
            if (negWords.contains(word.toLowerCase())) {
            	score -= 1   	
            } 
        }

        score match {
        	case s if s > 0 => 1	// Positive sentiment
        	case s if s < 0 => -1	// Negative sentiment
        	case s if s == 0 => 0	// Neutral sentiment	
        }
	}

	def readSentimentWords(path: String): Set[String] = Source.fromFile(path).getLines.toList.filterNot(_.contains(";")).tail.map(_.toLowerCase()).toSet

	def setupCredentials() = {
		// These lines set the system properties which twitter4j will use for authentication when using TwitterUtils.createStream(ssc, None)	
		val properties = Source.fromFile("./project.properties").getLines.toList
		
		val CONSUMERKEY = properties(0).split(" ")(2)
		val CONSUMERSECRET = properties(1).split(" ")(3)	
		val ACCESSTOKEN = properties(2).split(" ")(2)
		val ACCESSTOKENSECRET = properties(3).split(" ")(3)	

		System.setProperty("twitter4j.oauth.consumerKey", CONSUMERKEY)
		System.setProperty("twitter4j.oauth.consumerSecret", CONSUMERSECRET)
		System.setProperty("twitter4j.oauth.accessToken", ACCESSTOKEN)
		System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESSTOKENSECRET)
	}

}
