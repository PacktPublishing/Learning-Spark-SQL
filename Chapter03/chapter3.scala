//Code for Chapter 3: To be executed in Spark shell

//Code for Using Spark SQL for basic data analysis section
import org.apache.spark.sql.types._
import spark.implicits._

val age = StructField("age", DataTypes.IntegerType)
val job  = StructField("job", DataTypes.StringType)
val marital  = StructField("marital", DataTypes.StringType)
val edu  = StructField("edu", DataTypes.StringType)   
val credit_default  = StructField("credit_default", DataTypes.StringType) 
val housing  = StructField("housing", DataTypes.StringType)        
val loan  = StructField("loan", DataTypes.StringType)   
val contact  = StructField("contact", DataTypes.StringType)   
val month  = StructField("month", DataTypes.StringType)   
val day  = StructField("day", DataTypes.StringType)
val dur  = StructField("dur", DataTypes.DoubleType)   
val campaign  = StructField("campaign", DataTypes.DoubleType)   
val pdays  = StructField("pdays", DataTypes.DoubleType)   
val prev  = StructField("prev", DataTypes.DoubleType)     
val pout  = StructField("pout", DataTypes.StringType)   
val emp_var_rate  = StructField("emp_var_rate", DataTypes.DoubleType) 
val cons_price_idx  = StructField("cons_price_idx", DataTypes.DoubleType)    
val cons_conf_idx  = StructField("cons_conf_idx", DataTypes.DoubleType)          
val euribor3m  = StructField("euribor3m", DataTypes.DoubleType)  
val nr_employed  = StructField("nr_employed", DataTypes.DoubleType)                     
val deposit  = StructField("deposit", DataTypes.StringType)          

val fields = Array(age, job, marital, edu, credit_default, housing, loan, contact, month, day, dur, campaign, pdays, prev, pout, emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed, deposit)
val schema = StructType(fields)
val df = spark.read.schema(schema).option("sep", ";").option("header", true).csv("file:///Users/aurobindosarkar/Downloads/bank-additional/bank-additional-full.csv")
df.count()

case class Call(age: Int, job: String, marital: String, edu: String, credit_default: String, housing: String, loan: String, contact: String, month: String, day: String, dur: Double, campaign: Double, pdays: Double, prev: Double, pout: String, emp_var_rate: Double, cons_price_idx: Double, cons_conf_idx: Double, euribor3m: Double, nr_employed: Double, deposit: String)
val ds = df.as[Call]
ds.printSchema()

//Code for Identifying Missing Data section
val dfMissing = spark.read.schema(schema).option("sep", ";").option("header", true).csv("file:///Users/aurobindosarkar/Downloads/bank-additional/bank-additional-full-with-missing.csv")
val dsMissing = dfMissing.as[Call]
dsMissing.groupBy("marital").count().show()
dsMissing.groupBy("job").count().show()

//Code for Computing Basic Statistics section
case class CallStats(age: Int, dur: Double, campaign: Double, prev: Double, deposit: String)
val dsSubset = ds.select($"age", $"dur", $"campaign", $"prev", $"deposit")
dsSubset.show(5)
val dsCallStats = dsSubset.as[CallStats]
dsSubset.describe().show()
val cov = dsSubset.stat.cov("age","dur") 
println("age to call duration : Covariance = %.4f".format(cov))
val corr = dsSubset.stat.corr("age","dur") 
println("age to call duration : Correlation = %.4f".format(corr)) 

ds.stat.crosstab("age", "marital").orderBy("age_marital").show(10)
val freq = df.stat.freqItems(Seq("edu"), 0.3)
freq.collect()(0)
val quantiles = df.stat.approxQuantile("age", Array(0.25,0.5,0.75),0.0)
dsCallStats.cache()
import org.apache.spark.sql.expressions.scalalang.typed.{count => typedCount, avg => typedAvg, sum => typedSum}
(dsCallStats.groupByKey(callstats => callstats.deposit).agg(typedCount[CallStats](_.age).name("A"),typedAvg[CallStats](_.campaign).name("B"),typedAvg[CallStats](_.dur).name("C"),typedAvg[CallStats](_.prev).name("D")).withColumnRenamed("value", "E")).select($"E".name("TD Subscribed?"), $"A".name("Total Customers"), round($"B", 2).name("Avg calls(curr)"), round($"C", 2).name("Avg dur"), round($"D", 2).name("Avg calls(prev)")).show()
(dsCallStats.groupByKey(callstats => callstats.age).agg(typedCount[CallStats](_.age).name("A"),typedAvg[CallStats](_.campaign).name("B"),typedAvg[CallStats](_.dur).name("C"),typedAvg[CallStats](_.prev).name("D")).withColumnRenamed("value", "E")).select($"E".name("Age"), $"A".name("Total Customers"), round($"B", 2).name("Avg calls(curr)"), round($"C", 2).name("Avg dur"), round($"D", 2).name("Avg calls(prev)")).orderBy($"age").show(5)

//Code for Identifying Data Outliers section
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
val vectors = df.rdd.map(r => Vectors.dense(r.getDouble(10),r.getDouble(11), r.getDouble(12), r.getDouble(13)))
vectors.cache()
val kMeansModel = KMeans.train(vectors, 2, 20)
kMeansModel.clusterCenters.foreach(println)

//Follow the chapter for code on the Zeppelin section 

//Code for Sampling with Dataset API section
import scala.collection.immutable.Map
val fractions = Map("unknown" -> .10, "divorced" -> .15, "married" -> 0.5, "single" -> .25)
val dsStratifiedSample = ds.stat.sampleBy("marital", fractions, 36L)
dsStratifiedSample.count()
dsStratifiedSample.groupBy("marital").count().orderBy("marital").show()
val dsSampleWithReplacement = ds.sample(true, .10)
dsSampleWithReplacement.groupBy("marital").count().orderBy("marital").show()

//Code for Sampling with RDD API section
import org.apache.spark.mllib.linalg.Vector
val rowsRDD = df.rdd.map(r => (r.getAs[String](2), List(r.getInt(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5), r.getString(6), r.getString(7), r.getString(8), r.getString(9), r.getDouble(10), r.getDouble(11), r.getDouble(12), r.getDouble(13), r.getString(14), r.getDouble(15), r.getDouble(16), r.getDouble(17), r.getDouble(18), r.getDouble(19), r.getString(20))))
rowsRDD.take(2).foreach(println)
val fractions = Map("unknown" -> .10, "divorced" -> .15, "married" -> 0.5, "single" -> .25)
val rowsSampleRDD = rowsRDD.sampleByKey(true, fractions, 1)
val rowsSampleRDDExact = rowsRDD.sampleByKeyExact(true, fractions, 1)
println(rowsRDD.countByKey)
println(rowsSampleRDD.countByKey)
println(rowsSampleRDDExact.countByKey)

//Code for Using Spark SQL for creating pivot tables
val sourceDF = df.select($"job", $"marital", $"edu", $"housing", $"loan", $"contact", $"month", $"day", $"dur", $"campaign", $"pdays", $"prev", $"pout", $"deposit")
sourceDF.groupBy("marital").pivot("housing").agg(count("housing")).sort("marital").show()
sourceDF.groupBy("job").pivot("marital", Seq("unknown", "divorced", "married", "single")).agg(round(sum("campaign"), 2), round(avg("campaign"), 2)).sort("job").toDF("Job", "U-Tot", "U-Avg", "D-Tot", "D-Avg", "M-Tot", "M-Avg", "S-Tot", "S-Avg").show()
sourceDF.groupBy("job").pivot("marital", Seq("unknown", "divorced", "married", "single")).agg(round(sum("dur"), 2), round(avg("dur"), 2)).sort("job").toDF("Job", "U-Tot", "U-Avg", "D-Tot", "D-Avg", "M-Tot", "M-Avg", "S-Tot", "S-Avg").show()
sourceDF.groupBy("job").pivot("marital", Seq("divorced", "married")).agg(round(avg("dur"), 2)).sort("job").show()
sourceDF.groupBy("job", "housing").pivot("marital", Seq("divorced", "married")).agg(round(avg("dur"), 2)).sort("job").show

import org.apache.spark.sql._
val saveDF = sourceDF.groupBy("deposit").pivot("month", Seq("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")).agg(count("deposit")).sort("deposit").na.fill(0)
val writer: DataFrameWriter[Row] = saveDF.write
writer.format("csv").mode("overwrite").save("file:///Users/aurobindosarkar/Downloads/saveDF")
val dataRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/saveDF/*.csv").map(_.split(","))
val labels = List("deposit", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
val labelQ1 = List("jan", "feb", "mar")
val labelQ2 = List("apr", "may", "jun")
val labelQ3 = List("jul", "aug", "sep")
val labelQ4 = List("oct", "nov", "dec")
val indexQ1 = labelQ1.map(x => labels.indexOf(x))
val indexQ2 = labelQ2.map(x => labels.indexOf(x))
val indexQ3 = labelQ3.map(x => labels.indexOf(x))
val indexQ4 = labelQ4.map(x => labels.indexOf(x))
dataRDD.map(x => indexQ1.map(i => x(i).toDouble).sum).collect
dataRDD.map(x => indexQ2.map(i => x(i).toDouble).sum).collect
dataRDD.map(x => indexQ3.map(i => x(i).toDouble).sum).collect
dataRDD.map(x => indexQ4.map(i => x(i).toDouble).sum).collect





