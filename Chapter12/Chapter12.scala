//Code for Chapter 12 to be executed in Spark root_shell

//Code for Transforming data in ETL pipelines section
//This example requires Kafka commands to be executed at the appropriate places, please follow the instructions and sequence in the chapter.
val jsonDF = spark.read.json("file:///Users/aurobindosarkar/Downloads/cache-0-json")
jsonDF.printSchema()
val rawTweetsSchema = jsonDF.schema
val jsonString = rawTweetsSchema.json
val schema = DataType.fromJson(jsonString).asInstanceOf[StructType]
val rawTweets = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "tweetsa").load()
val parsedTweets = rawTweets.selectExpr("cast (value as string) as json").select(from_json($"json", schema).as("data")).select("data.*")

//************* Kafka commands here ************************//

val selectFields = parsedTweets.select("place.country").where($"place.country".isNotNull)
val s5 = selectFields.writeStream.outputMode("append").format("console").start()
val selectFields = parsedTweets.select("place.*").where($"place.country".isNotNull)
val selectFields = parsedTweets.select(struct("place.country_code", "place.name") as 'locationInfo).where($"locationInfo.country_code".isNotNull)
val selectFields = parsedTweets.select($"entities.hashtags" as 'tags).select('tags.getItem(0) as 'x).select($"x.indices" as 'y).select($"y".getItem(0) as 'z).where($"z".isNotNull)
val selectFields = parsedTweets.select($"entities.hashtags" as 'tags).select('tags.getItem(0) as 'x).select($"x.text" as 'y).where($"y".isNotNull)
val selectFields = parsedTweets.select($"entities.hashtags.indices" as 'tags).select(explode('tags))
val selectFields = parsedTweets.select($"entities.hashtags.indices".getItem(0) as 'tags).select(explode('tags))
val selectFields = parsedTweets.select(struct($"entities.media.type" as 'x, $"entities.media.url" as 'y) as 'z).where($"z.x".isNotNull).select(to_json('z) as 'c)


//Code for Addressing errors in ETL pipelines section
import spark.implicits._
import org.apache.spark.sql.types._
spark.read.json("file:///Users/aurobindosarkar/Downloads/test1.json").printSchema()
val schema = new StructType().add("a", "int").add("b", "double")
spark.read.option("header", true).schema(schema).csv("file:///Users/aurobindosarkar/Downloads/test1.csv").show()
spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json("file:///Users/aurobindosarkar/Downloads/test1.json").show()
spark.read.option("mode", "DROPMALFORMED").json("file:///Users/aurobindosarkar/Downloads/test1.json").show()
spark.read.option("mode", "FAILFAST").json("file:///Users/aurobindosarkar/Downloads/test1.json").show()
spark.read.option("wholeFile",true).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json("file:///Users/aurobindosarkar/Downloads/testMultiLine.json")
spark.read.option("multiLine",true).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json("file:///Users/aurobindosarkar/Downloads/testMultiLine.json")
spark.read.option("multiLine",true).option("header", true).csv("file:///Users/aurobindosarkar/Downloads/test1.csv").show()

//Code for Implementing a scalable monitoring solution section
//Uses Kafka so please follow the instructions in the book for executing the code in this section
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.streaming._

val schema = new StructType().add("clientIpAddress", "string").add("rfc1413ClientIdentity", "string").add("remoteUser", "string").add("dateTime", "string").add("zone", "string").add("request","string").add("httpStatusCode", "string").add("bytesSent", "string").add("referer", "string").add("userAgent", "string")
val rawRecords = spark.readStream.option("header", false).schema(schema).option("sep", " ").format("csv").load("file:///Users/aurobindosarkar/Downloads/NASA")
val ts = unix_timestamp(concat($"dateTime", lit(" "), $"zone"), "[dd/MMM/yyyy:HH:mm:ss Z]").cast("timestamp")
val logEvents = rawRecords.withColumn("ts", ts).withColumn("date", ts.cast(DateType)).select($"ts", $"date", $"clientIpAddress", concat($"dateTime", lit(" "), $"zone").as("original_dateTime"), $"request", $"httpStatusCode", $"bytesSent")
val query = logEvents.writeStream.outputMode("append").format("console").start()
val streamingETLQuery = logEvents.writeStream.trigger(Trigger.ProcessingTime("2 minutes")).format("parquet").partitionBy("date").option("path", "file:///Users/aurobindosarkar/Downloads/NASALogs").option("checkpointLocation", "file:///Users/aurobindosarkar/Downloads/NASALogs/checkpoint/").start()
val rawCSV = spark.readStream.schema(schema).option("latestFirst", "true").option("maxFilesPerTrigger", "5").option("header", false).option("sep", " ").format("csv").load("file:///Users/aurobindosarkar/Downloads/NASA")
val streamingETLQuery = logEvents.writeStream.trigger(Trigger.ProcessingTime("2 minutes")).format("json").partitionBy("date").option("path", "file:///Users/aurobindosarkar/Downloads/NASALogs").option("checkpointLocation", "file:///Users/aurobindosarkar/Downloads/NASALogs/checkpoint/").start()
val kafkaQuery = logEvents.selectExpr("CAST(ts AS STRING) AS key", "to_json(struct(*)) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "topica").option("checkpointLocation", "file:///Users/aurobindosarkar/Downloads/NASALogs/kafkacheckpoint/").start()
val kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "topica").option("startingOffsets", "earliest").load()
kafkaDF.printSchema()
val kafkaSchema = new StructType().add("ts", "timestamp").add("date", "string").add("clientIpAddress", "string").add("rfc1413ClientIdentity", "string").add("remoteUser", "string").add("original_dateTime", "string").add("request", "string").add("httpStatusCode", "string").add("bytesSent", "string")
val kafkaDF1 = kafkaDF.select(col("key").cast("string"), from_json(col("value").cast("string"), kafkaSchema).as("data")).select("data.*")
val kafkaQuery1 = kafkaDF1.select($"ts", $"date", $"clientIpAddress", $"original_dateTime", $"request", $"httpStatusCode", $"bytesSent").writeStream.outputMode("append").format("console").start()
val kafkaDF2 = spark.read.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe", "topica").load().selectExpr("CAST(value AS STRING) as myvalue")
kafkaDF2.registerTempTable("topicData3")
spark.sql("select myvalue from topicData3").take(3).foreach(println)
val parsed = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "topica").option("startingOffsets", "earliest").load().select(from_json(col("value").cast("string"), kafkaSchema).alias("parsed_value"))
val query = parsed.writeStream.outputMode("append").format("console").start()
val selectAllParsed = parsed.select("parsed_value.*")
val selectFieldsParsed = selectAllParsed.select("ts", "clientIpAddress", "request", "httpStatusCode")
val s1 = selectFieldsParsed.groupBy(window($"ts", "10 minutes", "5 minutes"), $"httpStatusCode").count().writeStream.outputMode("complete").format("console").start()
val s2 = selectFieldsParsed.groupBy(window($"ts", "10 minutes", "5 minutes"), $"request").count().writeStream.outputMode("complete").format("console").start()
val s4 = selectFieldsParsed.withWatermark("ts", "10 minutes").groupBy(window($"ts", "10 minutes", "5 minutes"), $"request").count().writeStream.outputMode("complete").format("console").start()

//Code for Deploying Spark machine learning pipelines section
//Follow the book for the sample pyspark session
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row

val sameModel = PipelineModel.load("file:///Users/aurobindosarkar/Downloads/spark-logistic-regression-model")
val test = spark.createDataFrame(Seq(
 (4L, "spark i j k"),
 (5L, "l m n"),
 (6L, "spark hadoop spark"),
 (7L, "apache hadoop")
 )).toDF("id", "text")

sameModel.transform(test).select("id", "text", "probability", "prediction").collect().foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) => println(s"($id, $text) --> prob=$prob, prediction=$prediction")}
val df = spark.read.parquet("file:///Users/aurobindosarkar/Downloads/spark-logistic-regression-model/stages/2_LogisticRegression_4abda37bdde1ddf65ea0/data/part-00000-415bf215-207a-4a49-985e-190eaf7253a7-c000.snappy.parquet")
df.show()

df.collect.foreach(println)

//Code for Understanding types of model scoring architectures section
//Follow the instructions in book to complete the non-Spark Scala parts
spark.read.parquet("file:///Users/aurobindosarkar/Downloads/spark-logistic-regression-model/stages/2_LogisticRegression_4abda37bdde1ddf65ea0/data/part-00000-415bf215-207a-4a49-985e-190eaf7253a7-c000.snappy.parquet").write.mode("overwrite").json("file:///Users/aurobindosarkar/Downloads/lr-model-json")
