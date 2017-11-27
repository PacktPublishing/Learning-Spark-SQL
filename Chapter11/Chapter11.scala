//Code for Chapter 11 to be executed in Spark shell

//Code for Optimizing data serialization
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import spark.implicits._
import spark.sessionState.conf
import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.util.SizeEstimator

//Code for Optimizing data serialization section
case class Bid(bidid: String, timestamp: String, ipinyouid: String, useragent: String, IP: String, region: Integer, cityID: Integer, adexchange: String, domain: String, turl: String, urlid: String, slotid: String, slotwidth: String, slotheight: String, slotvisibility: String, slotformat: String, slotprice: String, creative: String, bidprice: String) 
val bidEncoder = Encoders.product[Bid]
bidEncoder.schema
val bidExprEncoder = bidEncoder.asInstanceOf[ExpressionEncoder[Bid]]
bidExprEncoder.serializer
bidExprEncoder.namedExpressions 
val bidsDF = spark.read.format("csv").schema(bidEncoder.schema).option("sep", "\t").option("header", false).load("file:///Users/aurobindosarkar/Downloads/make-ipinyou-data-master/original-data/ipinyou.contest.dataset/bidfiles") 
bidsDF.take(1).foreach(println)
val bid = Bid("e3d962536ef3ac7096b31fdd1c1c24b0","20130311172101557","37a6259cc0c1dae299a7866489dff0bd","Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; QQDownload 734; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; eSobiSubscriber 2.0.4.16; MAAR),gzip(gfe),gzip(gfe)","219.232.120.*",1,1,"2","","DF9blS9bQqsIFYB4uA5R,b6c5272dfc63032f659be9b786c5f8da",null,"2006366309","728","90","1","0","5","5aca4c5f29e59e425c7ea657fdaac91e","300") 
val row = bidExprEncoder.toRow(bid) 
import org.apache.spark.sql.catalyst.dsl.expressions._ 
val attrs = Seq(DslSymbol('bidid).string, DslSymbol('timestamp).string, DslSymbol('ipinyouid).string, DslSymbol('useragent).string, DslSymbol('IP).string, DslSymbol('region).int, DslSymbol('cityID).int, DslSymbol('adexchange).string, DslSymbol('domain).string, DslSymbol('turl).string, DslSymbol('urlid).string, DslSymbol('slotid).string, DslSymbol('slotwidth).string, DslSymbol('slotheight).string, DslSymbol('slotvisibility).string, DslSymbol('slotformat).string, DslSymbol('slotprice).string, DslSymbol('creative).string, DslSymbol('bidprice).string) 
val getBackBid = bidExprEncoder.resolveAndBind(attrs).fromRow(row) 
bid == getBackBid 

//Code for Understanding the Dataset/DataFrame API section
val t1 = spark.range(7)
val t2 = spark.range(13)
val t3 = spark.range(19)
t1.explain() 
t1.explain(extended=true) 
t1.filter("id != 0").filter("id != 2").explain(true)
spark.sessionState.analyzer 
t1.filter("id != 0").filter("id != 2")

//Code for Understanding Catalyst transformations sections
val t0 = spark.range(0, 10000000) 
val df1 = t0.withColumn("uniform", rand(seed=10)) 
val df2 = t0.withColumn("normal", randn(seed=27)) 
df1.createOrReplaceTempView("t1") 
df2.createOrReplaceTempView("t2") 
spark.sql("SELECT sum(v) FROM (SELECT t1.id, 1 + t1.normal AS v FROM t1 JOIN t2 WHERE t1.id = t2.id AND t2.id > 5000000) tmp").explain(true) 

//Code for Visualizing Spark application execution section
val t1 = spark.range(7) 
val t2 = spark.range(13) 
val t3 = spark.range(19) 
val t4 = spark.range(1e8.toLong) 
val t5 = spark.range(1e8.toLong) 
val t6 = spark.range(1e3.toLong)  

val query = t1.join(t2).where(t1("id") === t2("id")).join(t3).where(t3("id") === t1("id")).explain() 
val query = t1.join(t2).where(t1("id") === t2("id")).join(t3).where(t3("id") === t1("id")).count()
val query = t4.join(t5).where(t4("id") === t5("id")).join(t6).where(t4("id") === t6("id")).explain() 

//Code for Understanding the CBO statistics collection section

sql("DESCRIBE EXTENDED customers").collect.foreach(println) 

//Code for Build side selection section
spark.sql("DROP TABLE IF EXISTS t1") 
spark.sql("DROP TABLE IF EXISTS t2") 
spark.sql("CREATE TABLE IF NOT EXISTS t1(id long, value long) USING parquet") 
spark.sql("CREATE TABLE IF NOT EXISTS t2(id long, value string) USING parquet") 
  
spark.range(5E8.toLong).select('id, (rand(17) * 1E6) cast "long").write.mode("overwrite").insertInto("t1") 
spark.range(1E8.toLong).select('id, 'id cast "string").write.mode("overwrite").insertInto("t2") 
 
sql("SELECT t1.id FROM t1, t2 WHERE t1.id = t2.id AND t1.value = 100").explain() 

//Code for Understanding multi-way JOIN ordering optimization section
sql("CREATE TABLE IF NOT EXISTS customers(id long, name string) USING parquet") 
sql("CREATE TABLE IF NOT EXISTS goods(id long, price long) USING parquet") 
sql("CREATE TABLE IF NOT EXISTS orders(customer_id long, good_id long) USING parquet") 
 
import org.apache.spark.sql.functions.rand 
 
spark.sql("CREATE TABLE IF NOT EXISTS customers(id long, name string) USING parquet") 
spark.sql("CREATE TABLE IF NOT EXISTS goods(id long, price long) USING parquet") 
spark.sql("CREATE TABLE IF NOT EXISTS orders(customer_id long, good_id long) USING parquet") 
 
spark.range(2E8.toLong).select('id, 'id cast "string").write.mode("overwrite").insertInto("customers") 
 
spark.range(1E8.toLong).select('id, (rand(17) * 1E6 + 2) cast "long").write.mode("overwrite").insertInto("goods") 
spark.range(1E7.toLong).select(rand(3) * 2E8 cast "long", (rand(5) * 1E8) cast "long").write.mode("overwrite").insertInto("orders") 

def benchmark(name: String)(f: => Unit) { 
      val startTime = System.nanoTime 
      f 
      val endTime = System.nanoTime 
      println(s"Time taken with $name: " + (endTime - 
                    startTime).toDouble / 1000000000 + " seconds") 
 } 

val conf = spark.sessionState.conf 
 
spark.conf.set("spark.sql.cbo.enabled", false) 
 
conf.cboEnabled 
conf.joinReorderEnabled 
benchmark("CBO OFF & JOIN REORDER DISABLED"){ sql("SELECT name FROM customers, orders, goods WHERE customers.id = orders.customer_id AND orders.good_id = goods.id AND goods.price > 1000000").show() }

spark.conf.set("spark.sql.cbo.enabled", true) 
conf.cboEnabled  
conf.joinReorderEnabled  
 
benchmark("CBO ON & JOIN REORDER DIABLED"){ sql("SELECT name FROM customers, orders, goods WHERE customers.id = orders.customer_id AND orders.good_id = goods.id AND goods.price > 1000000").show()} 

spark.conf.set("spark.sql.cbo.enabled", true) 
spark.conf.set("spark.sql.cbo.joinReorder.enabled", true) 
conf.cboEnabled 
conf.joinReorderEnabled 

scala> benchmark("CBO ON & JOIN REORDER ENABLED"){ sql("SELECT name FROM customers, orders, goods WHERE customers.id = orders.customer_id AND orders.good_id = goods.id AND goods.price > 1000000").show()} 

//Code for Understanding performance improvements using whole-stage code generation section
scala> sql("select count(*) from orders where customer_id = 26333955").explain() 
long count = 0; 
for (customer_id in orders) {  
   if (customer_id == 26333955) { 
         count += 1; 
   } 
} 

sql("EXPLAIN CODEGEN SELECT name FROM customers, orders, goods WHERE customers.id = orders.customer_id AND orders.good_id = goods.id AND goods.price > 1000000").take(1).foreach(println) 

spark.conf.set("spark.sql.codegen.wholeStage", false) 
 
conf.wholeStageEnabled 
 
val N = 20 << 20 
val M = 1 << 16 
 
val dim = broadcast(spark.range(M).selectExpr("id as k", "cast(id as string) as v")) 
 
benchmark("Join w long") { 
   spark.range(N).join(dim, (col("id") % M) === col("k")).count() 
 } 
 
spark.conf.set("spark.sql.codegen.wholeStage", true) 
 
conf.wholeStageEnabled 
val dim = broadcast(spark.range(M).selectExpr("id as k", "cast(id as string) as v")) 
 
benchmark("Join w long") { 
   spark.range(N).join(dim, (col("id") % M) === col("k")).count() 
 } 

val dim = broadcast(spark.range(M).selectExpr("id as k", "cast(id as string) as v")) 
benchmark("Join w long duplicated") { 
     val dim = broadcast(spark.range(M).selectExpr("cast(id/10 as long) as k")) 
     spark.range(N).join(dim, (col("id") % M) === col("k")).count() 
 } 
 
val dim3 = broadcast(spark.range(M).selectExpr("id as k1", "id as k2", "cast(id as string) as v")) 
benchmark("Join w 2 longs") { 
     spark.range(N).join(dim3, (col("id") % M) === col("k1") && (col("id") % M) === col("k2")).count() 
 } 
 
val dim4 = broadcast(spark.range(M).selectExpr("cast(id/10 as long) as k1", "cast(id/10 as long) as k2")) 
benchmark("Join w 2 longs duplicated") { 
     spark.range(N).join(dim4, (col("id") bitwiseAND M) === col("k1") && (col("id") bitwiseAND M) === col("k2")).count() 
 } 
 
val dim = broadcast(spark.range(M).selectExpr("id as k", "cast(id as string) as v")) 
benchmark("outer join w long") { 
     spark.range(N).join(dim, (col("id") % M) === col("k"), "left").count() 
 } 
 
val dim = broadcast(spark.range(M).selectExpr("id as k", "cast(id as string) as v")) 
benchmark("semi join w long") { 
     spark.range(N).join(dim, (col("id") % M) === col("k"), "leftsemi").count() 
 } 
 
val N = 2 << 20 
benchmark("merge join") { 
     val df1 = spark.range(N).selectExpr(s"id * 2 as k1") 
     val df2 = spark.range(N).selectExpr(s"id * 3 as k2") 
     df1.join(df2, col("k1") === col("k2")).count() 
 } 
 
val N = 2 << 20 
benchmark("sort merge join") { 
     val df1 = spark.range(N).selectExpr(s"(id * 15485863) % ${N*10} as k1") 
     val df2 = spark.range(N).selectExpr(s"(id * 15485867) % ${N*10} as k2") 
     df1.join(df2, col("k1") === col("k2")).count() 
 } 

scala> conf.getAllConfs.foreach(println) 
scala> conf.getAllDefinedConfs.foreach(println) 
