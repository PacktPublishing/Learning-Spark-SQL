import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Spark Streaming Example App
  */
object TFLStreamingApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TFLStreaming")
    val ssc = new StreamingContext(conf, Seconds(300))
    val stream = ssc.receiverStream(new TFLArrivalPredictionsByLine())
    println("Before")
    stream.print()
    println("After")
    if (args.length > 2) {
      stream.saveAsTextFiles(args(2))
    }
    ssc.start() 
    ssc.awaitTermination()
  }
}