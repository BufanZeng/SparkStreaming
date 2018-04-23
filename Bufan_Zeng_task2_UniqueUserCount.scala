
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object UniqueUserCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("UniqueUserCount")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(2))
        val line = ssc.socketTextStream("localhost", 9999)
        val result = line.map(row=>{
            val bin = row.toInt.toBinaryString
            bin.length - bin.reverse.toLong.toString.length
        }).reduce((a,b)=> math.max(a,b)).map(math.pow(2,_))
        result.print()
        ssc.start()
        ssc.awaitTermination()
    }
}