import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TrackHashTags {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("PopularHashTags")
        val sc = new SparkContext(conf)
        val consumerKey = args(0)       //KTXGxhBk5msRwWmddtRAlcnpL
        val consumerSecret = args(1)    //Jc7Jm5zDde28yNEkTCqYiDbvwVh2ipvkqCGur5RWjixM6eMwPE
        val accessToken = args(2)       //388486874-C1VSrgf2oRjJWywfSWoZawZdnt4dV9C3ZCj9CCWJ
        val accessTokenSecret = args(3) //41LfUoxo1CbQhoa2NwTAHCtaN8oXDcHAbMMtKtkAKMj5l
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
        System.setProperty("twitter4j.oauth.accessToken", accessToken)
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

        val ssc = new StreamingContext(sc, Seconds(2))
        val stream = TwitterUtils.createStream(ssc, None)

//        val lenrdd = stream.map(status => status.getText.split(" ").size).window(Seconds(120))
        val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#"))).window(Seconds(120))

        val topCounts120 = hashTags.map((_, 1)).reduceByKey(_ + _)
            .map { case (topic, count) => (count, topic) }
            .transform(_.sortByKey(false))

//        val avglen = lenrdd.reduce(_ + _)
        topCounts120.foreachRDD(rdd => {
            val topList = rdd.take(5)
            println("\nPopular topics in last 120 seconds (%s total):".format(rdd.count()))
            topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
        })
//        lenrdd.foreachRDD(rdd =>{
//            val total = rdd.sum()
//            val avg = total / rdd.count()
//            println("\nTotal tweets: " + total + ", Average length: " + avg)
//        })
        ssc.start()
        ssc.awaitTermination()
    }
}