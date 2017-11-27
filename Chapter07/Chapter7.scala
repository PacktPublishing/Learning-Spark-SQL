//Code for Chapter 7 to be executed in Spark shell
//./bin/spark-shell --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11

//Code for Exploring graphs using GraphFrames section
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.Row
import org.graphframes._

//Code for Constructing a GraphFrame section
val edgesRDD = spark.sparkContext.textFile("file:///Users/aurobindosarkar/Downloads/amzncopurchase/amazon0601.txt")
val schemaString = "src dst"
val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false))
val edgesSchema = new StructType(fields)
val rowRDD = edgesRDD.map(_.split("\t")).map(attributes => Row(attributes(0).trim, attributes(1).trim))
val edgesDF = spark.createDataFrame(rowRDD, edgesSchema)
val srcVerticesDF = edgesDF.select($"src").distinct
val destVerticesDF = edgesDF.select($"dst").distinct
val verticesDF = srcVerticesDF.union(destVerticesDF).distinct.select($"src".alias("id"))
edgesDF.count()
verticesDF.count()
val g = GraphFrame(verticesDF, edgesDF)

//Code for Basic graph queries and operations section
g.vertices.show(5)
g.edges.show(5)
g.inDegrees.show(5)
g.outDegrees.show(5)
g.edges.filter("src == 2").count()
g.edges.filter("src == 2").show()
g.edges.filter("dst == 2").show()
g.inDegrees.filter("inDegree >= 10").show()
g.inDegrees.groupBy("inDegree").count().sort(desc("inDegree")).show(5)
g.outDegrees.groupBy("outDegree").count().sort(desc("outDegree")).show(5)

//Code for Motif analysis using GraphFrames section
val motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show(5)
motifs.filter("b.id == 2").show()
val motifs3 = g.find("(a)-[e1]->(b); (a)-[e2]->(c)").filter("(b != c)")
motifs3.show(5)
val motifs3 = g.find("(a)-[]->(b); (a)-[]->(c)").filter("(b != c)")
motifs3.show()
motifs3.count()
val motifs3 = g.find("(a)-[]->(b); (a)-[]->(c); (b)-[]->(a)").filter("(b != c)")
motifs3.show()
motifs3.count()
val motifs3 = g.find("(a)-[]->(b); (c)-[]->(b)").filter("(a != c)")
motifs3.show(5)
motifs3.count()

val motifs3 = g.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(b)")
motifs3.show(5)
motifs3.count()

//Ensure you sufficient disk >100 GB and RAM >=14 GB or the following motifs4. 
//Alternatively you run the following on smaller subgraphs
val motifs4 = g.find("(a)-[e1]->(b); (c)-[e2]->(b); (c)-[e3]->(d)").filter("(a != c) AND (d != b) AND (d != a)")
motifs4.show(5)
motifs4.count()


//Code for Processing subgraphs using GraphFrames
val v2 = g.vertices.filter("src < 10")
val e2 = g.edges.filter("src < 10")
val g2 = GraphFrame(v2, e2)
g2.edges.groupBy("src").count().show()

val paths = g.find("(a)-[e]->(b)").filter("e.src < e.dst")
val e2 = paths.select("e.*")

//************************************
//sc.setCheckpointDir("hdfs://localhost:9000/cp")
//***********************************
//Code for Applying graph algorithms using GraphFrames section
val result = g.stronglyConnectedComponents.maxIter(10).run()
result.select("id", "component").groupBy("component").count().sort($"count".desc).show()

val results = g.triangleCount.run()
results.select("id", "count").show()

val results = g.pageRank.resetProbability(0.15).tol(0.01).run()
val prank = results.vertices.sort(desc("pagerank"))
prank.show(5)

val results = g.labelPropagation.maxIter(10).run()
results.select("id", "label").show()

val results = g.shortestPaths.landmarks(Seq("1110", "352")).run()
results.select("id", "distances").take(5).foreach(println)

//Code for Saving and loading GraphFrames section
g.vertices.write.mode("overwrite").parquet("hdfs://localhost:9000/gf/vertices")
g.edges.write.mode("overwrite").parquet("hdfs://localhost:9000/gf/edges")
val v = spark.read.parquet("hdfs://localhost:9000/gf/vertices") 
val e = spark.read.parquet("hdfs://localhost:9000/gf/edges")
val g = GraphFrame(v, e)

//Code for Analyzing a JSON dataset modeled as a graph section
//The downloaded input file needs to be processed before executing the following code.
//Use the Preprocess.java class for this purpose. 
//Open the source file and change the source and destination file paths. 
//And then compile and run the program to create the JSON format input file.
val df1 = spark.read.json("file:///Users/aurobindosarkar/Downloads/input.json")
df1.printSchema()
df1.take(5).foreach(println)
val x1=df1.select(df1.col("similarLines"))
df1.select(df1.col("similarLines.similar")).take(5).foreach(println)

df1.select(concat_ws(",", $"similarLines.similar")).take(5).foreach(println)
val flattened = df1.select($"ASIN", explode($"reviewLines.review").as("review_flat"))
flattened.show()
val flatReview  = flattened.select("ASIN", "review_flat.customerId")

val nodesDF = df1.select($"ASIN".alias("id"), $"Id".alias("productId"), $"title", $"ReviewMetaData", $"categories", $"categoryLines", $"group", $"reviewLines", $"salerank", $"similarLines", $"similars")
val edgesDF = df1.select($"ASIN".alias("src"), explode($"similarLines.similar").as("dst"))
val g = GraphFrame(nodesDF, edgesDF)
g.edges.filter("salerank < 100").count()
g.vertices.groupBy("group").count().show()
val v2 = g.vertices.filter("group = 'Book'")
v2.count()
val e2 = g.edges
e2.count()
val g2 = GraphFrame(v2, e2)
g2.vertices.count()
g2.edges.count()

val v2t = v2.select("id")
val e2t = v2t.join(e2, v2t("id") === e2("src"))
e2t.count()
val e2t1 = v2t.join(e2, v2t("id") === e2("src")).drop("id")
val e2t2 = v2t.join(e2t1, v2t("id") === e2t1("dst")).drop("id")
e2t2.count()
// "paths" contains vertex info. Extract the edges.

val es = g.edges.filter("salerank < 100")
es.count()
val e3 = es.select("src", "dst")
val g3 = GraphFrame(g.vertices, e3)
g3.vertices.count()
g3.edges.count()

val motifs = g3.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()
motifs.filter("b.ReviewMetaData.avg_rating > 4.0").show()
val paths = g3.find("(a)-[e]->(b)").filter("a.group = 'Book' AND b.group = 'Book'").filter("a.salerank < b.salerank")
val e2 = paths.select("e.src", "e.dst")
val g2 = GraphFrame(g.vertices, e2)
g2.vertices.take(5).foreach(println)
g2.edges.take(5).foreach(println)


import org.graphframes.lib.AggregateMessages
val AM = AggregateMessages
val msgToSrc = AM.dst("similars")
val msgToDst = AM.src("similars")
val agg = g.aggregateMessages.sendToSrc(msgToSrc).sendToDst(msgToDst).agg(sum(AM.msg).as("SummedSimilars"))
agg.show()

//Code for  Processing graphs containing multiple types of relationships section
val joinDF  = nodesDF.join(edgesDF).where(nodesDF("id") === edgesDF("src")).withColumn("relationship", when(($"similars" > 4) and ($"categories" <= 3), "highSimilars").otherwise("alsoPurchased"))
val edgesDFR = joinDF.select("src", "dst", "relationship")
val gDFR = GraphFrame(nodesDF, edgesDFR)
gDFR.edges.groupBy("relationship").count().show()
gDFR.edges.show()
val numHS = gDFR.edges.filter("relationship = 'highSimilars'").count()

val v2 = gDFR.vertices.filter("salerank < 2000000")
val e2 = gDFR.edges.filter("relationship = 'highSimilars'")
val g2 = GraphFrame(v2, e2)
val numEHS = g2.edges.count()
val numVHS = g2.vertices.count()

val paths = g.find("(a)-[e]->(b)").filter("e.relationship = 'highSimilars'").filter("a.group === b.group")


val bfsDF = gDFR.bfs.fromExpr("group = 'Book'").toExpr("categories < 3").edgeFilter("relationship != 'alsoPurchased'").maxPathLength(3).run()
bfsDF.take(2).foreach(println)

val v2 = gDFR.vertices.select("id", "group", "similars").filter("group = 'Book'")
val e2 = gDFR.edges.filter("relationship = 'highSimilars'")
val g2 = GraphFrame(v2, e2)
val numEHS = g2.edges.count()
val numVHS = g2.vertices.count()
val res1 = g2.find("(a)-[]->(b); (b)-[]->(c); !(a)-[]->(c)").filter("(a.group = c.group) and (a.similars = c.similars)")
val res2 = res1.filter("a.id != c.id").select("a.id", "a.group", "a.similars", "c.id", "c.group", "c.similars") 
res2.count()
res2.show(5)

val v2 = gDFR.vertices.select("id", "group", "title").filter("group = 'Book'")
val e2 = gDFR.edges.filter("relationship = 'highSimilars'")
val g2 = GraphFrame(v2, e2)
val results = g2.pageRank.resetProbability(0.15).tol(0.01).run()
val prank = results.vertices.sort(desc("pagerank"))
prank.take(10).foreach(println)

//Code for Viewing GraphFrame physical execution plan section
g.edges.filter("salerank < 100").explain()

//Code for Understanding partitioning in GraphFrames section
val v1 = g.vertices.select("id", "group").na.fill("unknown")
v1.show()
v1.groupBy("group").count().show
val g1 = GraphFrame(v1, g.edges)

val v2 = g.vertices.select("id", "group").na.fill("unknown")
val g2t1 = GraphFrame(v2, g.edges)
val g2t2 = g2t1.vertices.repartition(11, $"group")
val g2 = GraphFrame(g2t2, g.edges)
g1.vertices.show()
g2.vertices.show()

g1.vertices.rdd.partitions.size
g2.vertices.rdd.partitions.size

g1.vertices.write.mode("overwrite").csv("file:///Users/aurobindosarkar/Downloads/g1/partitions")
g2.vertices.write.mode("overwrite").csv("file:///Users/aurobindosarkar/Downloads/g2/partitions")


val g2c = g2.vertices.coalesce(5)
g2c.rdd.partitions.size
