//Code for Chapter 1
//To be executed in Spark 2.2 shell

//Code for Introducing SparkSession section
import org.apache.spark.sql.types._
spark.conf.set("spark.executor.cores", "2")
spark.conf.set("spark.executor.memory", "4g")

val recordSchema = new StructType().add("sample", "long").add("cThick", "integer").add("uCSize", "integer").add("uCShape", "integer").add("mAdhes", "integer").add("sECSize", "integer").add("bNuc", "integer").add("bChrom", "integer").add("nNuc", "integer").add("mitosis", "integer").add("clas", "integer")
//Replace directory for the input file with location of the file on your machine.
val df = spark.read.format("csv").option("header", false).schema(recordSchema).load("file:///Users/aurobindosarkar/Downloads/breast-cancer-wisconsin.data")
df.show()

df.createOrReplaceTempView("cancerTable")
val sqlDF = spark.sql("SELECT sample, bNuc from cancerTable") 
sqlDF.show()

case class CancerClass(sample: Long, cThick: Int, uCSize: Int, uCShape: Int, mAdhes: Int, sECSize: Int, bNuc: Int, bChrom: Int, nNuc: Int, mitosis: Int, clas: Int)
//Replace directory for the input file with location of the file on your machine.
val cancerDS = spark.sparkContext.textFile("file:///Users/aurobindosarkar/Documents/SparkBook/data/breast-cancer-wisconsin.data").map(_.split(",")).map(attributes => CancerClass(attributes(0).trim.toLong, attributes(1).trim.toInt, attributes(2).trim.toInt, attributes(3).trim.toInt, attributes(4).trim.toInt, attributes(5).trim.toInt, attributes(6).trim.toInt, attributes(7).trim.toInt, attributes(8).trim.toInt, attributes(9).trim.toInt, attributes(10).trim.toInt)).toDS()
def binarize(s: Int): Int = s match {case 2 => 0 case 4 => 1 }
spark.udf.register("udfValueToCategory", (arg: Int) => binarize(arg))
val sqlUDF = spark.sql("SELECT *, udfValueToCategory(clas) from cancerTable")
sqlUDF.show()

spark.catalog.currentDatabase
spark.catalog.isCached("cancerTable") 

spark.catalog.cacheTable("cancerTable")
spark.catalog.isCached("cancerTable") 
spark.catalog.clearCache
spark.catalog.listDatabases.show()

spark.catalog.listDatabases.take(1)
spark.catalog.listTables.show()
spark.catalog.dropTempView("cancerTable")
spark.catalog.listTables.show()

//Code for Understanding Resilient Distributed Datasets (RDDs) section
//Replace directory for the input file with location of the file on your machine.
val cancerRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/breast-cancer-wisconsin.data", 4)
cancerRDD.partitions.size
import spark.implicits._
val cancerDF = cancerRDD.toDF()
import org.apache.spark.sql.Row
def row(line: List[String]): Row = { Row(line(0).toLong, line(1).toInt, line(2).toInt, line(3).toInt, line(4).toInt, line(5).toInt, line(6).toInt, line(7).toInt, line(8).toInt, line(9).toInt, line(10).toInt) }
val data = cancerRDD.map(_.split(",").to[List]).map(row)
val cancerDF = spark.createDataFrame(data, recordSchema)

//Code for Understanding DataFrames and Datasets section
case class RestClass(name: String, street: String, city: String, phone: String, cuisine: String)
//Replace directory for the input files with location of the files on your machine.
val rest1DS = spark.sparkContext.textFile("file:///Users/aurobindosarkar/Documents/SparkBook/data/zagats.csv").map(_.split(",")).map(attributes => RestClass(attributes(0).trim, attributes(1).trim, attributes(2).trim, attributes(3).trim, attributes(4).trim)).toDS()
val rest2DS = spark.sparkContext.textFile("file:///Users/aurobindosarkar/Documents/SparkBook/data/fodors.csv").map(_.split(",")).map(attributes => RestClass(attributes(0).trim, attributes(1).trim, attributes(2).trim, attributes(3).trim, attributes(4).trim)).toDS()
def formatPhoneNo(s: String): String = s match {case s if s.contains("/") => s.replaceAll("/", "-").replaceAll("- ", "-").replaceAll("--", "-") case _ => s } 
val udfStandardizePhoneNos = udf[String, String]( x => formatPhoneNo(x) ) 
val rest2DSM1 = rest2DS.withColumn("stdphone", udfStandardizePhoneNos(rest2DS.col("phone")))
rest1DS.createOrReplaceTempView("rest1Table") 
rest2DSM1.createOrReplaceTempView("rest2Table")
spark.sql("SELECT count(*) from rest1Table, rest2Table where rest1Table.phone = rest2Table.stdphone").show()
val sqlDF = spark.sql("SELECT a.name, b.name, a.phone, b.stdphone from rest1Table a, rest2Table b where a.phone = b.stdphone")
sqlUDF.show()

//Code for Understanding Catalyst transformations section
case class PinTrans(bidid: String, timestamp: String, ipinyouid: String, useragent: String, IP: String, region: String, city: String, adexchange: String, domain: String, url:String, urlid: String, slotid: String, slotwidth: String, slotheight: String, slotvisibility: String, slotformat: String, slotprice: String, creative: String, bidprice: String) 
case class PinRegion(region: String, regionName: String)
//Replace directory for the input files with location of the files on your machine.
val pintransDF = spark.sparkContext.textFile("file:///Users/aurobindosarkar/Downloads/make-ipinyou-data-master/original-data/ipinyou.contest.dataset/training1st/bid.20130314.txt").map(_.split("\t")).map(attributes => PinTrans(attributes(0).trim, attributes(1).trim, attributes(2).trim, attributes(3).trim, attributes(4).trim, attributes(5).trim, attributes(6).trim, attributes(7).trim, attributes(8).trim, attributes(9).trim, attributes(10).trim, attributes(11).trim, attributes(12).trim, attributes(13).trim, attributes(14).trim, attributes(15).trim, attributes(16).trim, attributes(17).trim, attributes(18).trim)).toDF() 
val pinregionDF = spark.sparkContext.textFile("file:///Users/aurobindosarkar/Downloads/make-ipinyou-data-master/original-data/ipinyou.contest.dataset/region.en.txt").map(_.split("\t")).map(attributes => PinRegion(attributes(0).trim, attributes(1).trim)).toDF()

def benchmark(name: String)(f: => Unit) { 
   val startTime = System.nanoTime 
   f 
   val endTime = System.nanoTime 
   println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds") 
}

spark.conf.set("spark.sql.codegen.wholeStage", false) 
benchmark("Spark 1.6") { 
 pintransDF.join(pinregionDF, "region").count()  
}
spark.conf.set("spark.sql.codegen.wholeStage", true) 
benchmark("Spark 2.2") {  pintransDF.join(pinregionDF, "region").count() 
}

pintransDF.join(pinregionDF, "region").selectExpr("count(*)").explain(true) 

//Code for Introducing Project Tungsten section
pintransDF.join(pinregionDF, "region").selectExpr("count(*)").explain() 

//Code for Using Spark SQL for streaming applications section
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions._ 
import scala.concurrent.duration._ 
import org.apache.spark.sql.streaming.ProcessingTime 
import org.apache.spark.sql.streaming.OutputMode.Complete 

val bidSchema = new StructType().add("bidid", StringType).add("timestamp", StringType).add("ipinyouid", StringType).add("useragent", StringType).add("IP", StringType).add("region", IntegerType).add("city", IntegerType).add("adexchange", StringType).add("domain", StringType).add("url:String", StringType).add("urlid: String", StringType).add("slotid: String", StringType).add("slotwidth", StringType).add("slotheight", StringType).add("slotvisibility", StringType).add("slotformat", StringType).add("slotprice", StringType).add("creative", StringType).add("bidprice", StringType) 
val streamingInputDF = spark.readStream.format("csv").schema(bidSchema).option("header", false).option("inferSchema", true).option("sep", "\t").option("maxFilesPerTrigger", 1).load("file:///Users/aurobindosarkar/Downloads/make-ipinyou-data-master/original-data/ipinyou.contest.dataset/bidfiles")
val streamingCountsDF = streamingInputDF.groupBy($"city").count() 
val query = streamingCountsDF.writeStream.format("console").trigger(ProcessingTime(20.seconds)).queryName("counts").outputMode(Complete).start()
spark.streams.active.foreach(println) 
//Execute the following stop() method after you have executed the code in the next section (otherwise you will not see results of the code in the next section)
query.stop()

//Code for Understanding Structured Streaming Internals section
spark.streams.active(0).explain 








