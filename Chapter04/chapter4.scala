//Code for Chapter 4 to be executed in Spark spark-shell
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, Column, DataFrame}
import scala.collection.mutable.WrappedArray
import com.google.common.collect.ImmutableMap
import org.apache.spark.rdd.RDD

//Code for Pre-processing of the household electric consumption dataset sectoion
case class HouseholdEPC(date: String, time: String, gap: Double, grp: Double, voltage: Double, gi: Double, sm_1: Double, sm_2: Double, sm_3: Double)
val hhEPCRdd = sc.textFile("file:///Users/aurobindosarkar/Downloads/household_power/household_power_consumption.txt")
hhEPCRdd.count()
val header = hhEPCRdd.first()
val data = hhEPCRdd.filter(row => row != header).filter(rows => !rows.contains("?"))
val hhEPCClassRdd = data.map(_.split(";")).map(p => HouseholdEPC(p(0).trim().toString,p(1).trim().toString,p(2).toDouble,p(3).toDouble,p(4).toDouble,p(5).toDouble,p(6).toDouble,p(7).toDouble,p(8).toDouble))
val hhEPCDF = hhEPCClassRdd.toDF()
hhEPCDF.show(5)
hhEPCDF.count()

//Code Computing basic statistics and aggregations section
hhEPCDF.describe().show()
hhEPCDF.describe().select($"summary", $"gap", $"grp", $"voltage", $"gi", $"sm_1", $"sm_2", $"sm_3", round($"gap", 4).name("rgap"), round($"grp", 4).name("rgrp"), round($"voltage", 4).name("rvoltage"), round($"gi", 4).name("rgi"), round($"sm_1", 4).name("rsm_1"), round($"sm_2", 4).name("rsm_2"), round($"sm_3", 4).name("rsm_3")).drop("gap", "grp", "voltage", "gi", "sm_1", "sm_2", "sm_3").show()
val numDates = hhEPCDF.groupBy("date").agg(countDistinct("date")).count()

//Code for Augmenting the dataset section
val hhEPCDatesDf = hhEPCDF.withColumn("dow", from_unixtime(unix_timestamp($"date", "dd/MM/yyyy"), "EEEEE")).withColumn("day", dayofmonth(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp")))).withColumn("month", month(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp")))).withColumn("year", year(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp"))))
hhEPCDatesDf.show(5)

//Code for Executing other miscellaneous processing steps section
val delTmDF = hhEPCDF.drop("time")
val finalDayDf1 = delTmDF.groupBy($"date").agg(sum($"gap").name("A"),sum($"grp").name("B"),avg($"voltage").name("C"),sum($"gi").name("D"), sum($"sm_1").name("E"), sum($"sm_2").name("F"), sum($"sm_3").name("G")).select($"date", round($"A", 2).name("dgap"), round($"B", 2).name("dgrp"), round($"C", 2).name("dvoltage"), round($"C", 2).name("dgi"), round($"E", 2).name("dsm_1"), round($"F", 2).name("dsm_2"), round($"G", 2).name("dsm_3")).withColumn("day", dayofmonth(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp")))).withColumn("month", month(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp")))).withColumn("year", year(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp"))))
finalDayDf1.show(5)
val readingsByMonthDf = hhEPCDatesDf.groupBy($"year", $"month").count().orderBy($"year", $"month")
readingsByMonthDf.count()
readingsByMonthDf.show(5)
case class HouseholdEPCDTmDay(date: String, day: String, month: String, year: String, dgap: Double, dgrp: Double, dvoltage: Double, dgi: Double, dsm_1: Double, dsm_2: Double, dsm_3: Double)
val finalDayDs1 = finalDayDf1.as[HouseholdEPCDTmDay]

//Code for Pre-processing of the weather dataset section
case class DayWeather(CET: String, Max_TemperatureC: Double, Mean_TemperatureC: Double, Min_TemperatureC: Double, Dew_PointC: Double, MeanDew_PointC: Double, Min_DewpointC: Double, Max_Humidity: Double, Mean_Humidity: Double, Min_Humidity: Double, Max_Sea_Level_PressurehPa: Double, Mean_Sea_Leve_PressurehPa: Double, Min_Sea_Level_PressurehPa: Double, Max_VisibilityKm: Double, Mean_VisibilityKm: Double, Min_VisibilitykM: Double, Max_Wind_SpeedKmph: Double, Mean_Wind_SpeedKmph: Double, Max_Gust_SpeedKmph: Double, Precipitationmm: Double, CloudCover: Double, Events: String, WindDirDegrees: Double)
val dwRdd1 = sc.textFile("file:///Users/aurobindosarkar/Downloads/Paris_Weather/Paris_Weather_Year_1.csv")
val dwRdd2 = sc.textFile("file:///Users/aurobindosarkar/Downloads/Paris_Weather/Paris_Weather_Year_2.csv")
val dwRdd3 = sc.textFile("file:///Users/aurobindosarkar/Downloads/Paris_Weather/Paris_Weather_Year_3.csv")
val dwRdd4 = sc.textFile("file:///Users/aurobindosarkar/Downloads/Paris_Weather/Paris_Weather_Year_4.csv")
println("Number 0f Readings - Year 1: " + dwRdd1.count())
println("Number 0f Readings - Year 2: " + dwRdd2.count())
println("Number 0f Readings - Year 3: " + dwRdd3.count())
println("Number 0f Readings - Year 4: " + dwRdd4.count())
val header = dwRdd1.first()
val data1 = dwRdd1.filter(row => row != header)
val data2 = dwRdd2.filter(row => row != header)
val data3 = dwRdd3.filter(row => row != header)
val data4 = dwRdd4.filter(row => row != header)

//Code for Analyzing missing data section
val emptyFieldRowsRDD = data1.map(_.split(",")).filter(!_.contains(""))
emptyFieldRowsRDD.count()
val csvDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load("file:///Users/aurobindosarkar/Downloads/Paris_Weather/Paris_Weather_Year_1.csv")
csvDF.select($"CET", $" Events").show()
val dropRowsWithEmptyFieldsDF = csvDF.filter($" Events" =!= "").filter($" Max Gust SpeedKm/h" =!= "")
dropRowsWithEmptyFieldsDF.count()
def processRdd(data: RDD[String]): RDD[DayWeather] = { val dwClassRdd = data.map(_.split(",")).map(c => c.map(f => f match { case x if x.isEmpty() => "0"; case x => x })).map(p => DayWeather(p(0).trim().toString, p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble, p(9).toDouble, p(10).toDouble, p(11).toDouble, p(12).toDouble, p(13).toDouble, p(14).toDouble, p(15).toDouble, p(16).toDouble, p(17).toDouble, p(18).toDouble, p(19).toDouble, p(20).toDouble, p(21).trim().toString, p(22).toDouble)); dwClassRdd; }
val dwClassRdd1 = processRdd(data1)
dwClassRdd1.take(5).foreach(println)
val dwClassRdd2 = processRdd(data2)
val dwClassRdd3 = processRdd(data3)
val dwClassRdd4 = processRdd(data4)
val dwDS1 = dwClassRdd1.toDF().na.replace(Seq("CET", "Events"),Map("0" -> "NA")).as[DayWeather]
val dwDS2 = dwClassRdd2.toDF().na.replace(Seq("CET", "Events"),Map("0" -> "NA")).as[DayWeather]
val dwDS3 = dwClassRdd3.toDF().na.replace(Seq("CET", "Events"),Map("0" -> "NA")).as[DayWeather]
val dwDS4 = dwClassRdd4.toDF().na.replace(Seq("CET", "Events"),Map("0" -> "NA")).as[DayWeather]
val finalDs2 = dwDS1.union(dwDS2).union(dwDS3).union(dwDS4)
finalDs2.count()

//Code for Combining data using the JOIN operation section
val joinedDF = finalDayDs1.join(finalDs2).where(unix_timestamp(finalDayDs1("date"), "dd/MM/yyyy") === unix_timestamp(finalDs2("CET"), "yyyy-MM-dd"))
joinedDF.count()
val corr = joinedDF.stat.corr("Mean_TemperatureC","dgap") 
println("Mean_TemperatureC to grp : Correlation = %.4f".format(corr)) 
val corr = joinedDF.stat.corr("Mean_TemperatureC","dgrp") 
println("Mean_TemperatureC to dgrp : Correlation = %.4f".format(corr)) 
val corr = joinedDF.stat.corr("Mean_Humidity","dgap") 
val corr = joinedDF.stat.corr("Mean_Humidity","dgrp") 
val corr = joinedDF.stat.corr("Max_TemperatureC","dsm_1") 
val corr = joinedDF.stat.corr("Max_TemperatureC","dsm_2")
val corr = joinedDF.stat.corr("Max_TemperatureC","dsm_3")
val joinedMonthlyDF = joinedDF.groupBy("year", "month").agg(sum($"dgap").name("A"),sum($"dgrp").name("B"),avg($"dvoltage").name("C"),sum($"dgi").name("D"), sum($"dsm_1").name("E"), sum($"dsm_2").name("F"), sum($"dsm_3").name("G")).select($"year", $"month", round($"A", 2).name("mgap"), round($"B", 2).name("mgrp"), round($"C", 2).name("mvoltage"), round($"C", 2).name("mgi"), round($"E", 2).name("msm_1"), round($"F", 2).name("msm_2"), round($"G", 2).name("msm_3")).orderBy("year", "month")
val joinedDayDF = finalDayDs1.join(finalDs2).where(unix_timestamp(finalDayDs1("date"), "dd/MM/yyyy") === unix_timestamp(finalDs2("CET"), "yyyy-MM-dd"))
joinedDayDF.count()
val joinedDayDowDF = joinedDayDF.withColumn("dow", from_unixtime(unix_timestamp($"date", "dd/MM/yyyy"), "EEEEE"))
joinedDayDowDF.printSchema()

//Code for Munging Textual Data section
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.DataFrame
import spark.implicits._

//Code for Processing multiple input data files section
val path = "hdfs://localhost:9000/20_newsgroups/comp.graphics"
val data = spark.sparkContext.wholeTextFiles(path)
var output : org.apache.spark.rdd.RDD[(String, String, Int)] = sc.emptyRDD
val files = data.map { case (filename, content) => filename}
def Process(filename: String): org.apache.spark.rdd.RDD[(String, String, Int)]= {
    println("Processing:" + filename)
    val fpath = filename.split('/').last;
    val lines = spark.sparkContext.textFile(filename);
    val counts = lines.flatMap(line => line.split(" ")).map(word => word).map(word => (word, 1)).reduceByKey(_ + _); 
    val word_counts = counts.map( x => (fpath,x._1, x._2));   
    word_counts
}
val buf = scala.collection.mutable.ArrayBuffer.empty[org.apache.spark.rdd.RDD[(String, String, Int)]]
output = spark.sparkContext.union(buf.toList);
files.collect.foreach( filename => { buf += Process(filename); })
output.cache()
output.take(5).foreach(println)

//Code for Removing stop words section
val stopWords = sc.broadcast(Set("as", "able", "about", "above", "according", "accordingly", "across", "actually", "..."))
def processLine(s: String, stopWords: Set[String]): List[String] = { 
    s.toLowerCase()
      .split("\\s+")
      .filter(x => x.matches("[A-Za-z]+"))
      .filter(!stopWords.contains(_))
      .toList
}
val groupedRDD = output.map{ case (x, y, z) => (x, (processLine(y.trim(), stopWords.value)).mkString, z)}.filter{case (x, y, z) => !y.equals("")}
groupedRDD.take(5).foreach(println)
val words = groupedRDD.map{ case (x, y, z) => y}
val wordsDF = words.toDF
val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
val stopwords = sc.textFile("file:///Users/aurobindosarkar/Downloads/sparkworks/SparkBook/data/stopwords.txt")
val stopwordsDF = stopwords.flatMap(line => line.split("[\\s]")).map(word => regex.replaceAllIn(word.trim.toLowerCase, "")).filter(word => !word.isEmpty).toDF()
val cleanwords = wordsDF.except(stopwordsDF)
stopwordsDF.count()
words.count()
cleanwords.count()

//Code for Munging time series data section
//bin/spark-shell --jars /Users/aurobindosarkar/Downloads/spark-timeseries-master/target/sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import com.cloudera.sparkts._
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

//Code for Pre-processing of the time-series dataset section
val amznRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/yahoo/tableAMZN.csv")
val orclRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/yahoo/tableORCL.csv")
val ibmRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/yahoo/tableIBM.csv")
val cscoRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/yahoo/tableCSCO.csv")
val googRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/yahoo/tableGOOG.csv")
val msftRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/yahoo/tableMSFT.csv")
case class Stock(ticker: String, datestr: String, open: Double, high: Double, low: Double, close: Double, volume: Int, adjclose: Double)
val header = amznRDD.first()
val amznData = amznRDD.filter(row => row != header).map(_.split(",")).map(p => Stock("AMZN", p(0).trim().toString, p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toInt, p(6).toDouble)).toDF.as[Stock]
val orclData = orclRDD.filter(row => row != header).map(_.split(",")).map(p => Stock("ORCL", p(0).trim().toString, p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toInt, p(6).toDouble)).toDF.as[Stock]
val ibmData = ibmRDD.filter(row => row != header).map(_.split(",")).map(p => Stock("IBM", p(0).trim().toString, p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toInt, p(6).toDouble)).toDF.as[Stock]
val cscoData = cscoRDD.filter(row => row != header).map(_.split(",")).map(p => Stock("CSCO", p(0).trim().toString, p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toInt, p(6).toDouble)).toDF.as[Stock]
val googData = googRDD.filter(row => row != header).map(_.split(",")).map(p => Stock("GOOG", p(0).trim().toString, p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toInt, p(6).toDouble)).toDF.as[Stock]
val msftData = msftRDD.filter(row => row != header).map(_.split(",")).map(p => Stock("MSFT", p(0).trim().toString, p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toInt, p(6).toDouble)).toDF.as[Stock]
val allData = amznData.union(orclData).union(ibmData).union(cscoData).union(googData).union(msftData)

//Code for Processing date fields section
val allWithDMY = allData.withColumn("day", dayofmonth(to_date(unix_timestamp($"datestr", "yyyy-MM-dd").cast("timestamp")))).withColumn("month", month(to_date(unix_timestamp($"datestr", "yyyy-MM-dd").cast("timestamp")))).withColumn("year", year(to_date(unix_timestamp($"datestr", "yyyy-MM-dd").cast("timestamp"))))

//Code for Persisting and loading data section
allWithDMY.write.mode("overwrite").csv("file:///Users/aurobindosarkar/Downloads/dtDF")
def loadObservations(sqlContext: SQLContext, path: String): DataFrame = { 
    val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
    val tokens = line.split(',');
    val dt = ZonedDateTime.of(tokens(10).toInt, tokens(9).toInt, tokens(8).toInt, 0, 0, 0, 0, ZoneId.systemDefault());
    val ticker = tokens(0).toString;
    val open = tokens(2).toDouble;
    val high = tokens(3).toDouble;
    val low = tokens(4).toDouble;
    val close = tokens(5).toDouble;
    val volume = tokens(6).toInt;
    val adjclose = tokens(7).toDouble;
    Row(Timestamp.from(dt.toInstant), ticker, open, high, low, close, volume, adjclose);
   }
   val fields = Seq(StructField("timestamp", TimestampType, true), StructField("ticker", StringType, true), StructField("open", DoubleType, true), StructField("high", DoubleType, true), StructField("low", DoubleType, true), StructField("close", DoubleType, true), StructField("volume", IntegerType, true), StructField("adjclose", DoubleType, true));
   val schema = StructType(fields);
   sqlContext.createDataFrame(rowRdd, schema);
}
val tickerObs = loadObservations(spark.sqlContext, "file:///Users/aurobindosarkar/Downloads/dtDF")
tickerObs.show(5)

//Code for Defining a date-time index section
val zone = ZoneId.systemDefault()
val dtIndex = DateTimeIndex.uniformFromInterval(ZonedDateTime.of(LocalDateTime.parse("2015-12-04T00:00:00"), zone), ZonedDateTime.of(LocalDateTime.parse("2016-12-04T00:00:00"), zone), new BusinessDayFrequency(1)) 

//Code for Using the  TimeSeriesRDD object section
val tickerTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, tickerObs, "timestamp", "ticker", "close")
tickerTsrdd.cache()
println(tickerTsrdd.count())
tickerTsrdd.take(2).foreach(println)

//Code for Handling missing time-series data section
val filled = tickerTsrdd.fill("linear")

//Code for Computing basic statistics section
val stats = filled.seriesStats()
stats.foreach(println)

//Code for Dealing with variable length records section
val inputRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/ZZAlpha1/combined.txt")
inputRDD.count()
inputRDD.take(5).foreach(println)
//Code for Converting variable-length records to fixed-length records section
def countSubstring( str:String, substr:String ) = substr.r.findAllMatchIn(str).length
def nSubs(substr: String) = udf((x: String) => countSubstring(x, substr))
val nCommas = inputRDD.toDF().withColumn("commas", nSubs(",")($"value"))
nCommas.describe().select($"summary", $"commas").where(($"summary" === "count") || ($"summary" === "max")).show()
nCommas.take(5).foreach(println)
def insertSubstring( str:String, substr:String, times: Int ): String = { val builder = StringBuilder.newBuilder; builder.append(str.substring(0, str.lastIndexOf(substr)+1));builder.append(substr * (22- times));builder.append(str.substring(str.lastIndexOf(substr)+1)); builder.toString;}
def nInserts(substr: String) = udf((x: String, times: Int) => insertSubstring(x, substr, times))
val fixedLengthDf = nCommas.withColumn("record", nInserts(",")($"value", $"commas")).drop("value", "commas")
fixedLengthDf.take(5).foreach(println)
fixedLengthDf.count()
val dupRemovedDf = fixedLengthDf.dropDuplicates()
dupRemovedDf.count()
case class Portfolio(datestr: String, ls: String, stock1: String, stock2: String, stock3: String, stock4: String, stock5: String, stock6: String, stock7: String, stock8: String, stock9: String, stock10: String, stock11: String, stock12: String, stock13: String, stock14: String, stock15: String, stock16: String, stock17: String, stock18: String, stock19: String, stock20: String, avgstr: String )
val rowsRdd = dupRemovedDf.rdd.map{ row: Row => row.getString(0).split(",") }
val dfFixed = rowsRdd.map(s => Portfolio(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10), s(11), s(12), s(13), s(14), s(15), s(16), s(17), s(18), s(19), s(20), s(21), s(22))).toDF()
dfFixed.count()
dfFixed.select("datestr", "ls", "stock1", "stock2", "avgstr").show(5)
val df2 = dfFixed.na.replace("stock2",ImmutableMap.of("", "NA")).select("datestr", "ls", "stock1", "stock2", "avgstr")
df2.show(5)

//Code for Extracting data from "messy" columns section
val df3 = dfFixed.select("datestr", "ls", "stock1", "avgstr")
df3.take(5).foreach(println)
def replaceFirstSubstring( str:String, substr:String, repl: String): String = { val result = str.replaceFirst(substr, repl); result;}
def nRepls(substr: String, repl: String) = udf((x: String) => replaceFirstSubstring(x, substr, repl))
val df4 = df3.withColumn("cleanDatestr", nRepls("_", " ")($"datestr")).drop("datestr").withColumnRenamed("cleanDatestr", "datestr").select("datestr", "ls", "stock1", "avgstr")
df4.take(5).foreach(println)
val df4A = df4.withColumn("temp1", nRepls("=", "")($"stock1")).withColumn("temp", nRepls("/", " ")($"temp1")).drop("temp1", "stock1")
df4A.take(5).foreach(println)
def splitString( str:String, sep:String): Array[String] = { val result = str.split(sep); result;}
def splitStr(sep: String) = udf((x: String) => splitString(x.trim(), sep))
val df5 = df4A.withColumn("temp1", splitStr(" ")($"datestr")).withColumn("temp2", splitStr(" ")($"temp")).withColumn("avgarray", splitStr(" ")($"avgstr")).drop("datestr", "temp").select("temp1", "ls", "temp2", "avgarray")
df5.show(5)
def retArrayIndex( str:Array[String], index:Int): String = { val result = str(index); result;}
def retArrayVal(index: Int) = udf((x: WrappedArray[String]) => retArrayIndex(x.toArray[String], index))
val df6 = df5.withColumn("month", retArrayVal(0)($"temp1")).withColumn("dom", retArrayVal(1)($"temp1")).withColumn("year", retArrayVal(2)($"temp1")).withColumn("y4", retArrayVal(3)($"temp1")).withColumn("file", retArrayVal(4)($"temp1")).withColumn("ticker", retArrayVal(0)($"temp2")).withColumn("S/P Ratio", retArrayVal(1)($"temp2")).withColumn("SP", retArrayVal(2)($"temp2")).withColumn("PP", retArrayVal(3)($"temp2")).withColumn("nStocks", retArrayVal(2)($"avgarray")).drop("temp1", "temp2", "avgarray")
df6.show(5)
def extractUptoSecondSubstring( str:String, sub:String): String = { val result = str.substring(0, str.indexOf(sub, str.indexOf(sub) + 1)); result;}
def extractStr(sub: String) = udf((x: String) => extractUptoSecondSubstring(x.trim(), sub))
val df6A = df6.withColumn("type", extractStr("_")($"file")).drop("file").select("month", "dom", "year", "y4", "type", "ls", "ticker", "S/P Ratio", "SP", "PP", "nStocks")
df6A.show(5)



//Code for Preparing data for machine learning section
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors

//Code for Pre-processing data for machine learning section
def containsSubstring( str:String, substr:String): Double = { if (str.contains(substr)) 1 else 0}
def udfContains(substr: String) = udf((x: String) => containsSubstring(x, substr))
def udfVec() = udf[org.apache.spark.ml.linalg.Vector, String, Int, Double, Double, Double] { (a, b, c, d, e) => val x = a match { case "Monday" => 1; case "Tuesday" => 2; case "Wednesday" => 3; case "Thursday" => 4; case "Friday" => 5; case "Saturday" => 6; case "Sunday" => 7; }; Vectors.dense(x, b, c, d, e);}
val joinedRained = joinedDayDowDF.withColumn("label", udfContains("Rain")($"Events")).withColumn("features", udfVec()($"dow", $"month", $"dsm_1", $"dsm_2", $"dsm_3"))
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(joinedRained)
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(7).fit(joinedRained)
val Array(trainingData, testData) = joinedRained.randomSplit(Array(0.7, 0.3))

//Code for Creating and running a machine learning pipeline section
val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(10)
// Convert indexed labels back to original labels.
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
// Chain indexers and forest in a Pipeline.
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
// Train model. This also runs the indexers.
val model = pipeline.fit(trainingData)
// Make predictions.
val predictions = model.transform(testData)
// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)
// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))
val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
println("Learned classification forest model:\n" + rfModel.toDebugString)





