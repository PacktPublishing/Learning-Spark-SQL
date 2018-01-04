//Code for Chapter 5
//Code for Building Spark streaming applications section
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.concurrent.duration._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.OutputMode.Complete
import spark.implicits._

val bidSchema = new StructType().add("bidid", StringType).add("timestamp", StringType).add("ipinyouid", StringType).add("useragent", StringType).add("IP", StringType).add("region", IntegerType).add("cityID", IntegerType).add("adexchange", StringType).add("domain", StringType).add("turl", StringType).add("urlid", StringType).add("slotid", StringType).add("slotwidth", StringType).add("slotheight", StringType).add("slotvisibility", StringType).add("slotformat", StringType).add("slotprice", StringType).add("creative", StringType).add("bidprice", StringType)
val streamingInputDF = spark.readStream.format("csv").schema(bidSchema).option("header", false).option("inferSchema", true).option("sep", "\t").option("maxFilesPerTrigger", 1).load("file:///Users/aurobindosarkar/Downloads/make-ipinyou-data-master/original-data/ipinyou.contest.dataset/bidfiles")
streamingInputDF.printSchema()

//Code for Implementing sliding window-based functionality section
val ts = unix_timestamp($"timestamp", "yyyyMMddHHmmssSSS").cast("timestamp")
val streamingCityTimeDF = streamingInputDF.withColumn("ts", ts).select($"cityID", $"ts")
//Wait for the output show on the screen after the next statement
val windowedCounts = streamingCityTimeDF.groupBy(window($"ts", "10 minutes", "5 minutes"), $"cityID").count().writeStream.outputMode("complete").format("console").start()

//Code for Joining a streaming dataset with a static dataset section
val citySchema = new StructType().add("cityID", StringType).add("cityName", StringType)
val staticDF = spark.read.format("csv").schema(citySchema).option("header", false).option("inferSchema", true).option("sep", "\t").load("file:///Users/aurobindosarkar/Downloads/make-ipinyou-data-master/original-data/ipinyou.contest.dataset/city.en.txt")
val joinedDF = streamingCityTimeDF.join(staticDF, "cityID")
//Wait for the output show on the screen after the next statement
val windowedCityCounts = joinedDF.groupBy(window($"ts", "10 minutes", "5 minutes"), $"cityName").count().writeStream.outputMode("complete").format("console").start()
val streamingCityNameBidsTimeDF = streamingInputDF.withColumn("ts", ts).select($"ts", $"bidid", $"cityID", $"bidprice", $"slotprice").join(staticDF, "cityID")
//Wait for the output show on the screen after the next statement
val cityBids = streamingCityNameBidsTimeDF.select($"ts", $"bidid", $"bidprice", $"slotprice", $"cityName").writeStream.outputMode("append").format("console").start()


//Code for Using the Dataset API in Structured Streaming section
case class Bid(bidid: String, timestamp: String, ipinyouid: String, useragent: String, IP: String, region: Integer, cityID: Integer, adexchange: String, domain: String, turl: String, urlid: String, slotid: String, slotwidth: String, slotheight: String, slotvisibility: String, slotformat: String, slotprice: String, creative: String, bidprice: String)
val ds = streamingInputDF.as[Bid]

//Code for Using the Foreach Sink for arbitrary computations on output section
import org.apache.spark.sql.ForeachWriter
val writer = new ForeachWriter[String] {
  override def open(partitionId: Long, version: Long) = true
  override def process(value: String) = println(value)
  override def close(errorOrNull: Throwable) = {}
}
val dsForeach = ds.filter(_.adexchange == "3").map(_.useragent).writeStream.foreach(writer).start()

//Code for Using the Memory Sink to save output to a table section
val aggAdexchangeDF = streamingInputDF.groupBy($"adexchange").count()
//Wait for the output show on the screen after the next statement
val aggQuery = aggAdexchangeDF.writeStream.queryName("aggregateTable").outputMode("complete").format("memory").start()
spark.sql("select * from aggregateTable").show()

//Code for Using the File Sink to save output to a partitioned table section
val cityBidsParquet = streamingCityNameBidsTimeDF.select($"bidid", $"bidprice", $"slotprice", $"cityName").writeStream.outputMode("append").format("parquet").option("path", "hdfs://localhost:9000/pout").option("checkpointLocation", "hdfs://localhost:9000/poutcp").start()

//Code for Monitoring streaming queries section
spark.streams.active.foreach(x => println("ID:"+ x.id + "             Run ID:"+ x.runId + "               Status: "+ x.status))

// get the unique identifier of the running query that persists across restarts from checkpoint data
windowedCounts.id          
// get the unique id of this run of the query, which will be generated at every start/restart
windowedCounts.runId       
// the exception if the query has been terminated with error
windowedCounts.exception       
// the most recent progress update of this streaming query
windowedCounts.lastProgress  

windowedCounts.stop()

//Code for Using Kafka with Spark Structured Streaming
//Refer book for steps to be executed for Kafka and ZooKeeper
//Proper sequence of steps need to be followed for execution of code in this section
val ds1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test").load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

val query = ds1.writeStream.outputMode("append").format("console").start()
val ds2 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "connect-test").load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
val query = ds2.writeStream.outputMode("append").format("console").start()

//Follow the instructions in the book to compile & execute code for Writing a receiver for a custom data source section


