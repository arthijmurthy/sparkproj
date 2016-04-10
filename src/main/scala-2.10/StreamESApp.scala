/**
 * Created by Arthi on 4/3/16.
 */

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.elasticsearch.hadoop.mr.EsOutputFormat
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
// sqlcontext
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
// Hadoop imports
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, Text, NullWritable}




object IndexTweetsLive {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage IndexTweetsLive <master> <key> <secret key> <access token> <access token secret>  <es-resource> [es-nodes]")
    }
    // Configure Twitter credentials below
    val consumerKey = ""
    val consumerSecret = ""
    val accessToken = ""
    val accessTokenSecret = ""

    //val Array(master, consumerKey, consumerSecret, accessToken, accessTokenSecret, esResource) = args.take(6)
    val esNodes = args.length match {
      case x: Int if x > 6 => args(6)
      case _ => "localhost"
    }

    SharedIndex.setupTwitter(consumerKey, consumerSecret, accessToken, accessTokenSecret)

    val ssc = new StreamingContext(new SparkConf().setMaster("local[2]").setAppName("IndexTweetsLive"), Seconds(1))

    val tweets = TwitterUtils.createStream(ssc, None)
    tweets.print()
    //Save to ES
    tweets.foreachRDD{(tweetRDD, time) =>
      val sc = tweetRDD.context
      val sqlCtx = new SQLContext(sc)
      import sqlCtx.createDataFrame
      val tweetsAsCS = createDataFrame(tweetRDD.map(SharedIndex.prepareTweetsCaseClass))
      tweetsAsCS.saveToEs("spark/tweets")
    }
    println("sscstart")
    ssc.start()
    println("awaittermination")
    ssc.awaitTermination()
    println("complete")
  }
}
