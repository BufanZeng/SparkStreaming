import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object AveTweetLength {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("AveTweetLength")
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

        val lenrdd = stream.map(status => status.getText.length).window(Seconds(120))
        lenrdd.foreachRDD(rdd =>{
            val total = rdd.sum()
            val avg = total / rdd.count()
            println("\nTotal tweets: " + total + ", Average length: " + avg)
        })
        ssc.start()
        ssc.awaitTermination()
    }
}