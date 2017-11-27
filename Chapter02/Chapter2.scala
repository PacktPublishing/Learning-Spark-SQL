//Code for Chapter 2
//For some sections, please follow the sequence of execution in the book. For example, in the MySQL section - certain commands need to be executed on MySQL.

// This file contains Scala code to be executed in Spark shell only.
//Code for Using Spark with relational data section. Please follow the step-wise instructions in the book. 
//This sect
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.util.Properties
val inFileRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/UCI Online Retail.txt")
val allRowsRDD = inFileRDD.map(line =>line.split("\t").map(_.trim))
val header = allRowsRDD.first
val data = allRowsRDD.filter(_(0) != header(0))
val fields = Seq(
StructField("invoiceNo", StringType, true),
StructField("stockCode", StringType, true),
StructField("description", StringType, true),
StructField("quantity", IntegerType, true),
StructField("invoiceDate", StringType, true),
StructField("unitPrice", DoubleType, true),
StructField("customerID", StringType, true),
StructField("country", StringType, true)
)
val schema = StructType(fields)
val rowRDD = data.map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3).toInt, attributes(4), attributes(5).toDouble, attributes(6), attributes(7)))
val r1DF = spark.createDataFrame(rowRDD, schema)
val ts = unix_timestamp($"invoiceDate","dd/MM/yyHH:mm").cast("timestamp")
val r2DF = r1DF.withColumn("ts", ts)
r2DF.show()
r2DF.createOrReplaceTempView("retailTable")
val r3DF = spark.sql("select * from retailTable where ts< '2011-12-01'")
val r4DF = spark.sql("select * from retailTable where ts>= '2011-12-01'")
val selectData = r4DF.select("invoiceNo", "stockCode", "description", "quantity", "unitPrice", "customerID", "country", "ts")
val writeData = selectData.withColumnRenamed("ts", "invoiceDate")
writeData.show()

//Dependency on MySQL part.
val dbUrl = "jdbc:mysql://localhost:3306/retailDB"
val prop = new Properties()
prop.setProperty("user", "retaildbuser")
prop.setProperty("password", "mypass")
writeData.write.mode("append").jdbc(dbUrl, "transactions", prop)
//End of MySQL Dependency

scala>val selectData = r3DF.select("invoiceNo", "stockCode", "description", "quantity", "unitPrice", "customerID", "country", "ts")
scala>val writeData = selectData.withColumnRenamed("ts", "invoiceDate")
scala>writeData.select("*").write.format("json").save("hdfs://localhost:9000/Users/r3DF")

//Code for Using Spark with MongoDB section
//Download the connector and then start Spark shell as shown below:
//./bin/spark-shell --jars /Users/aurobindosarkar/Downloads/mongo-spark-connector_2.11-2.2.0-assembly.jar
//This section has dependencies on certain steps to be done in MongoDB environment
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
val readConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/nycschoolsDB.schools?readPreference=primaryPreferred"))
val writeConfig = WriteConfig(Map("uri" -> "mongodb://localhost:27017/nycschoolsDB.outCollection"))
case class School(dbn: String, school_name: String, boro: String, building_code: String, phone_number: String, fax_number: String, grade_span_min: String, grade_span_max: String, expgrade_span_min: String, expgrade_span_max: String, bus: String, subway: String, primary_address_line_1: String, city: String, state_code: String, zip: String, website: String, total_students: String, campus_name: String, school_type: String, overview_paragraph: String, program_highlights: String, language_classes: String, advancedplacement_courses: String, online_ap_courses: String, online_language_courses: String, extracurricular_activities: String, psal_sports_boys: String, psal_sports_girls: String, psal_sports_coed: String, school_sports: String, partner_cbo: String, partner_hospital: String, partner_highered: String, partner_cultural: String, partner_nonprofit: String, partner_corporate: String, partner_financial: String, partner_other: String, addtl_info1: String, addtl_info2: String, start_time: String, end_time: String, se_services: String, ell_programs: String, school_accessibility_description: String, number_programs: String, priority01: String, priority02: String, priority03: String, priority04: String, priority05: String, priority06: String, priority07: String, priority08: String, priority09: String, priority10: String, Location_1: String)
val schoolsDF = MongoSpark.load(sc, readConfig).toDF[School]
schoolsDF.take(1).foreach(println)

//Code for Using Spark with JSON data section
val reviewsDF = spark.read.json("file:///Users/aurobindosarkar/Downloads/reviews_Electronics_5.json")
reviewsDF.printSchema()
reviewsDF.createOrReplaceTempView("reviewsTable")
val selectedDF = spark.sql("SELECT asin, overall, reviewTime, reviewerID, reviewerName FROM reviewsTable WHERE overall >= 3")
selectedDF.show()
val selectedJSONArrayElementDF = reviewsDF.select($"asin", $"overall", $"helpful").where($"helpful".getItem(0) < 3)
selectedJSONArrayElementDF.show()

//Code for Using Spark with Avro files. You will need to shift to Spark 2.1 for this section due to a reported bug in the spark-avro connector.
//Start Spark shell as shown below:
//Aurobindos-MacBook-Pro-2:spark-2.1.0-bin-hadoop2.7 aurobindosarkar$ bin/spark-shell --jars /Users/aurobindosarkar/Downloads/spark-avro_2.11-3.2.0.jar
import com.databricks.spark.avro._
val reviewsDF = spark.read.json("file:///Users/aurobindosarkar/Downloads/reviews_Electronics_5.json")
reviewsDF.count()
reviewsDF.filter("overall < 3").coalesce(1).write.avro("file:///Users/aurobindosarkar/Downloads/amazon_reviews/avro")
val reviewsAvroDF = spark.read.avro("file:///Users/aurobindosarkar/Downloads/amazon_reviews/avro/part-00000-c6b6b423-70d6-440f-acbe-0de65a6a7f2e.avro")
reviewsAvroDF.count()
reviewsAvroDF.select("asin", "helpful", "overall", "reviewTime", "reviewerID", "reviewerName").show(5)
spark.conf.set("spark.sql.avro.compression.codec", "deflate")
spark.conf.set("spark.sql.avro.deflate.level", "5")
val reviewsAvroDF = spark.read.avro("file:////Users/aurobindosarkar/Downloads/amazon_reviews/avro/part-00000-c6b6b423-70d6-440f-acbe-0de65a6a7f2e.avro")
reviewsAvroDF.write.partitionBy("overall").avro("file:////Users/aurobindosarkar/Downloads/amazon_reviews/avro/partitioned")

//Using Spark with Parquet files
reviewsDF.filter("overall < 3").coalesce(1).write.parquet("file:///Users/aurobindosarkar/Downloads/amazon_reviews/parquet")
val reviewsParquetDF = spark.read.parquet("file:///Users/aurobindosarkar/Downloads/amazon_reviews/parquet/part-00000-3b512935-ec11-48fa-8720-e52a6a29416b.snappy.parquet")
reviewsParquetDF.createOrReplaceTempView("reviewsTable")
val reviews1RatingsDF = spark.sql("select asin, overall, reviewerID, reviewerName from reviewsTable where overall < 2")
reviews1RatingsDF.show(5, false)



































