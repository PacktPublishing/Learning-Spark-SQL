//Code for Chapter 6 to be executed in Spark Shell

//Code for Implementing a Spark ML classification model section.
import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrameNaFunctions, Row}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, IndexToString, VectorIndexer, OneHotEncoder, PCA, Binarizer, VectorSlicer, StandardScaler, Bucketizer, ChiSqSelector, Normalizer }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, BinaryClassificationEvaluator}
import org.apache.spark.sql.functions.{ sum,when , row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier, LogisticRegression, DecisionTreeClassificationModel, DecisionTreeClassifier, GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.linalg.{Vector, Vectors}


//Code for Exploring the diabetes Dataset section
val inDiaDataDF = spark.read.option("header", true).csv("file:///Users/aurobindosarkar/Downloads/dataset_diabetes/diabetic_data.csv").cache()
inDiaDataDF.printSchema()
inDiaDataDF.take(5).foreach(println)
inDiaDataDF.select("num_lab_procedures", "num_procedures", "num_medications", "number_diagnoses").describe().show()
inDiaDataDF.select($"weight").groupBy($"weight").count().select($"weight", (($"count" / inDiaDataDF.count())*100).alias("percent_recs")).where("weight = '?'").show()
inDiaDataDF.select($"payer_code").groupBy($"payer_code").count().select($"payer_code", (($"count" / inDiaDataDF.count())*100).alias("percent_recs")).where("payer_code = '?'").show()
inDiaDataDF.select($"medical_specialty").groupBy($"medical_specialty").count().select($"medical_specialty", (($"count" / inDiaDataDF.count())*100).alias("percent_recs")).where("medical_specialty = '?'").show()
val diaDataDrpDF = inDiaDataDF.drop("weight", "payer_code")
diaDataDrpDF.select($"patient_nbr").groupBy($"patient_nbr").count().where("count > 1").show(5)
val w = Window.partitionBy($"patient_nbr").orderBy($"encounter_id".desc)
val diaDataSlctFirstDF = diaDataDrpDF.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")
diaDataSlctFirstDF.select($"patient_nbr").groupBy($"patient_nbr").count().where("count > 1").show()
diaDataSlctFirstDF.count()
val diaDataAdmttedDF = diaDataSlctFirstDF.filter($"discharge_disposition_id" =!= "11")
diaDataAdmttedDF.count()
 
val admTypeId = StructField("admTypeId", DataTypes.IntegerType)
val admType = StructField("admType",    DataTypes.StringType)
val fields = Array(admTypeId, admType)
val schema = StructType(fields)
val admTypeDF = spark.read.option("header", true).schema(schema).csv("file:///Users/aurobindosarkar/Downloads/dataset_diabetes/admission_type.csv")
admTypeDF.printSchema()
admTypeDF.take(5).foreach(println)

val admSrcId = StructField("admSrcId", DataTypes.IntegerType)
val admSrc = StructField("admSrc",    DataTypes.StringType)
val fields = Array(admSrcId, admSrc)
val schema = StructType(fields)
val admSrcDF = spark.read.option("header", true).csv("file:///Users/aurobindosarkar/Downloads/dataset_diabetes/admission_source.csv")
admSrcDF.printSchema()
admSrcDF.take(5).foreach(println)

val dchrgDispId = StructField("dchrgDispId", DataTypes.IntegerType)
val dchrgDisp = StructField("dchrgDisp",    DataTypes.StringType)
val fields = Array(dchrgDispId, dchrgDisp)
val schema = StructType(fields)
val dchrgDispDF = spark.read.option("header", true).schema(schema).csv("file:///Users/aurobindosarkar/Downloads/dataset_diabetes/discharge_disposition.csv")
dchrgDispDF.printSchema()
dchrgDispDF.take(5).foreach(println)
val joinDF = diaDataAdmttedDF.join(dchrgDispDF, diaDataAdmttedDF("discharge_disposition_id") === dchrgDispDF("dchrgDispId")).withColumnRenamed("description", "discharge_disposition").drop(dchrgDispDF("dchrgDispId")).join(admTypeDF, diaDataAdmttedDF("admission_type_id") === admTypeDF("admTypeId")).withColumnRenamed("description", "admission_type").drop(admTypeDF("admTypeId")).join(admSrcDF, diaDataAdmttedDF("admission_source_id") === admSrcDF("admission_source_id")).withColumnRenamed("description", "admission_source").drop(admSrcDF("admission_source_id"))
joinDF.select("encounter_id", "dchrgDisp", "admType", "admission_source").show(5)
joinDF.select("encounter_id", "dchrgDisp").groupBy("dchrgDisp").count().orderBy($"count".desc).take(5).foreach(println)
joinDF.select("encounter_id", "admType").groupBy("admType").count().orderBy($"count".desc).take(5).foreach(println)
joinDF.select("encounter_id", "admission_source").groupBy("admission_source").count().orderBy($"count".desc).take(5).foreach(println)

//Code for Preprocessing the data section
diaDataAdmttedDF.select("medical_specialty").where("medical_specialty = '?'").groupBy("medical_specialty").count().show()
val diaDataRplcMedSplDF = diaDataAdmttedDF.na.replace("medical_specialty", Map("?" -> "Missing"))
val diaDataDrpColsDF = diaDataRplcMedSplDF.drop("encounter_id", "patient_nbr", "diag_2", "diag_3", "max_glu_serum", "metformin", "repaglinide", "nateglinide", "chlorpropamide", "glimepiride", "acetohexamide", "glipizide", "glyburide", "tolbutamide", "pioglitazone", "rosiglitazone", "acarbose", "miglitol", "troglitazone", "tolazamide", "examide", "citoglipton", "insulin", "glyburide-metformin", "glipizide-metformin", "glimepiride-pioglitazone", "metformin-rosiglitazone", "metformin-pioglitazone")
diaDataDrpColsDF.groupBy($"A1Cresult").count().show()

def udfA1CGrps() = udf[Double, String] { a => val x = a match { case "None" => 1.0; case ">8" => 2.0; case ">7" => 3.0; case "Norm" => 4.0;}; x;} 

val diaDataA1CResultsDF = diaDataDrpColsDF.withColumn("A1CResGrp", udfA1CGrps()($"A1Cresult"))

diaDataA1CResultsDF.groupBy("A1CResGrp").count().withColumn("Percent_of_Population", ($"count" / diaDataA1CResultsDF.count())*100).withColumnRenamed("count", "Num_of_Encounters").show()

def udfReAdmBins() = udf[String, String] { a => val x = a match { case "<30" => "Readmitted"; case "NO" => "Not Readmitted"; case ">30" => "Not Readmitted";}; x;}

val diaDataReadmtdDF = diaDataA1CResultsDF.withColumn("Readmitted", udfReAdmBins()($"readmitted"))

diaDataReadmtdDF.groupBy("race").pivot("Readmitted").agg(count("Readmitted")).show()
diaDataReadmtdDF.groupBy("A1CResGrp").pivot("Readmitted").agg(count("Readmitted")).orderBy("A1CResGrp").show()
diaDataReadmtdDF.groupBy("gender").pivot("Readmitted").agg(count("Readmitted")).show()
def udfAgeBins() = udf[String, String] { a => val x = a match { case "[0-10)" => "Young"; case "[10-20)" => "Young"; case "[20-30)" => "Young"; case "[30-40)" => "Middle"; case "[40-50)" => "Middle"; case "[50-60)" => "Middle"; case "[60-70)" => "Elder";  case "[70-80)" => "Elder"; case "[80-90)" => "Elder"; case "[90-100)" => "Elder";}; x;}
val diaDataAgeBinsDF = diaDataReadmtdDF.withColumn("age_category", udfAgeBins()($"age"))
val diaDataRmvGndrDF = diaDataAgeBinsDF.filter($"gender" =!= "Unknown/Invalid")
val diaDataFinalDF = diaDataRmvGndrDF.select($"race", $"gender", $"age_category", $"admission_type_id".cast(IntegerType), $"discharge_disposition_id".cast(IntegerType), $"admission_source_id".cast(IntegerType), $"time_in_hospital".cast(IntegerType), $"num_lab_procedures".cast(DoubleType), $"num_procedures".cast(IntegerType), $"num_medications".cast(IntegerType), $"number_outpatient".cast(IntegerType), $"number_emergency".cast(IntegerType), $"number_inpatient".cast(IntegerType), $"diag_1", $"number_diagnoses".cast(IntegerType), $"A1CResGrp", $"change", $"diabetesMed", $"Readmitted").withColumnRenamed("age_category", "age")
diaDataFinalDF.printSchema()
diaDataFinalDF.take(5).foreach(println)

//Code for Using StringIndexer for indexing categorical features and labels section
val raceIndexer = new StringIndexer().setInputCol("race").setOutputCol("raceCat").fit(diaDataFinalDF)
raceIndexer.transform(diaDataFinalDF).select("race", "raceCat").show()
raceIndexer.transform(diaDataFinalDF).select("race", "raceCat").groupBy("raceCat").count().show()
val raceIndexer = new StringIndexer().setInputCol("race").setOutputCol("raceCat").fit(diaDataFinalDF)
val rDF = raceIndexer.transform(diaDataFinalDF)
val genderIndexer = new StringIndexer().setInputCol("gender").setOutputCol("genderCat").fit(rDF)
val gDF = genderIndexer.transform(rDF)
val ageCategoryIndexer  = new StringIndexer().setInputCol("age").setOutputCol("ageCat").fit(gDF)
val acDF = ageCategoryIndexer.transform(gDF)
val A1CresultIndexer  = new StringIndexer().setInputCol("A1CResGrp").setOutputCol("A1CResGrpCat").fit(acDF)
val a1crDF = A1CresultIndexer.transform(acDF)
val changeIndexer  = new StringIndexer().setInputCol("change").setOutputCol("changeCat").fit(a1crDF)
val cDF = changeIndexer.transform(a1crDF)
val diabetesMedIndexer  = new StringIndexer().setInputCol("diabetesMed").setOutputCol("diabetesMedCat").fit(cDF)
val dmDF = diabetesMedIndexer.transform(cDF)
dmDF.printSchema()
val catFeatColNames = Seq("race", "gender", "age", "A1CResGrp", "change", "diabetesMed")
val stringIndexers = catFeatColNames.map { colName =>
  new StringIndexer()
    .setInputCol(colName)
    .setOutputCol(colName + "Cat")
    .fit(diaDataFinalDF)
}
val labelIndexer = new StringIndexer().setInputCol("Readmitted").setOutputCol("indexedLabel")

//Code for Using VectorAssembler for assembling features into one column section
val dataDF = dmDF.stat.sampleBy("Readmitted", Map("Readmitted" -> 1.0, "Not Readmitted" -> .030), 0)
val assembler = new VectorAssembler().setInputCols(Array("num_lab_procedures", "num_procedures", "num_medications", "number_outpatient", "number_emergency", "number_inpatient", "number_diagnoses", "admission_type_id", "discharge_disposition_id", "admission_source_id", "time_in_hospital", "raceCat", "genderCat", "ageCat", "A1CresultCat", "changeCat", "diabetesMedCat")).setOutputCol("features")

val numFeatNames = Seq("num_lab_procedures", "num_procedures", "num_medications", "number_outpatient", "number_emergency", "number_inpatient", "number_diagnoses", "admission_type_id", "discharge_disposition_id", "admission_source_id", "time_in_hospital")
val catFeatNames = catFeatColNames.map(_ + "Cat")
val allFeatNames = numFeatNames ++ catFeatNames
val assembler = new VectorAssembler().setInputCols(Array(allFeatNames: _*)).setOutputCol("features")
val df2 = assembler.transform(dataDF)

val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(df2)

//Code for Using a classifier section
val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(10)
val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
val gbt = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10)

//Code for Creating the pipeline section
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf))

//Code for Creating the training and test datasets section
val Array(trainingData, testData) = df2.randomSplit(Array(0.8, 0.2), 11L)
val model = pipeline.fit(trainingData)

//Code for Making predictions section
val predictions = model.transform(testData)
predictions.select("prediction", "indexedLabel", "features").show(25)
val predictionAndLabels = predictions.select("prediction","indexedLabel").rdd.map { row =>
      (row.get(0).asInstanceOf[Double],row.get(1).asInstanceOf[Double])
}
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))
val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
println("Learned classification forest model:\n" + rfModel.toDebugString)

//Code for Selecting the best model section
val paramGrid = new ParamGridBuilder().addGrid(rf.maxBins, Array(25, 28, 31)).addGrid(rf.maxDepth, Array(4, 6, 8)).addGrid(rf.impurity, Array("entropy", "gini")).build()
val evaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel")
val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(2)
val crossValidatorModel = cv.fit(df2)
val predictions = crossValidatorModel.transform(testData)
predictions.select("prediction", "indexedLabel", "features").show(25)
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

//Code for Changing the ML algorithm in the pipeline section
val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr))
val Array(trainingData, testData) = df2.randomSplit(Array(0.8, 0.2), 11L)
val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)
predictions.select($"indexedLabel", $"prediction").where("indexedLabel != prediction").count()

//Code for Using Principal Component Analysis to select features section
val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(3).fit(df2)
val result = pca.transform(df2).select("pcaFeatures")

//Code for Using encoders section
val indexer = new StringIndexer().setInputCol("race").setOutputCol("raceIndex").fit(df2)
val indexed = indexer.transform(df2)
val encoder = new OneHotEncoder().setInputCol("raceIndex").setOutputCol("raceVec")
val encoded = encoder.transform(indexed)
encoded.select("raceVec").show()

//Code for Using Bucketizer section
val splits = Array(Double.NegativeInfinity, 20.0, 40.0, 60.0, 80.0, 100.0, Double.PositiveInfinity)
val bucketizer = new Bucketizer().setInputCol("num_lab_procedures").setOutputCol("bucketedLabProcs").setSplits(splits)
//Transform original data into its bucket index.
val bucketedData = bucketizer.transform(df2)

println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
bucketedData.select("num_lab_procedures", "bucketedLabProcs")show()

//Code for Using VectorSlicer section
val slicer = new VectorSlicer().setInputCol("features").setOutputCol("slicedfeatures").setNames(Array("raceCat", "genderCat", "ageCat", "A1CResGrpCat"))
val output = slicer.transform(df2)
output.select("slicedFeatures").take(5).foreach(println)
val slicer = new VectorSlicer().setInputCol("features").setOutputCol("slicedfeatures").setNames(Array("raceCat", "genderCat", "ageCat"))
val output = slicer.transform(df2)
output.select("slicedFeatures").take(5).foreach(println)

//Code for Using Chi-squared selector
def udfReAdmLabels() = udf[Double, String] { a => val x = a match { case "Readmitted" => 1.0; case "Not Readmitted" => 0.0;}; x;}
val df3 = df2.withColumn("reAdmLabel", udfReAdmLabels()($"Readmitted"))
val selector = new ChiSqSelector().setNumTopFeatures(1).setFeaturesCol("features").setLabelCol("reAdmLabel").setOutputCol("selectedFeatures")
val result = selector.fit(df3).transform(df3)
println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
result.select("selectedFeatures").show()

//Code for Standardizing data using Normalizer
val normalizer = new Normalizer().setInputCol("raw_features ").setOutputCol("features") 

//Code for Retrieving our original labels section
val labelIndexer = new StringIndexer().setInputCol("Readmitted").setOutputCol("indexedLabel").fit(df2)
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(df2)
val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3))
val gbt = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10)
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))
// Train model. This also runs the indexers.
val model = pipeline.fit(trainingData)
// Make predictions.
val predictions = model.transform(testData)
predictions.select("predictedLabel", "indexedLabel", "features").show(5)

//Code for Implementing a Spark ML clustering model section
def udfLabels() = udf[Integer, String] { a => val x = a match { case "very_low" => 0; case "Very Low" => 0; case "Low" => 0; case "Middle" => 1; case "High" => 1;}; x;}
val inDataDF = spark.read.option("header", true).csv("file:///Users/aurobindosarkar/Downloads/Data_User_Modeling.csv").withColumn("label", udfLabels()($"UNS"))
inDataDF.select("label").groupBy("label").count().show()
inDataDF.cache()

val inDataFinalDF = inDataDF.select($"STG".cast(DoubleType), $"SCG".cast(DoubleType), $"STR".cast(DoubleType), $"LPR".cast(DoubleType), $"PEG".cast(DoubleType), $"UNS", $"label")
inDataFinalDF.count()
inDataFinalDF.take(5).foreach(println)
inDataFinalDF.printSchema()

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
val allFeatNames = Seq("STG", "SCG", "STR", "LPR", "PEG")
val assembler = new VectorAssembler().setInputCols(Array(allFeatNames: _*)).setOutputCol("features")
val df2 = assembler.transform(inDataFinalDF)
df2.cache()
// Trains a k-means model.
val kmeans = new KMeans().setK(2).setSeed(1L)
val model = kmeans.fit(df2)
println(kmeans.explainParams)
val WSSSE = model.computeCost(df2)
println(s"Within Set Sum of Squared Errors = $WSSSE")
model.clusterCenters.foreach(println)

val transformed =  model.transform(df2)
transformed.take(5).foreach(println)
transformed.select("prediction").groupBy("prediction").count().orderBy("prediction").show()
transformed.take(5).foreach(println)
transformed.select("label", "prediction").show()
//val yDF = xDF.select($"numLabel", $"prediction").where("numLabel != prediction")
val y1DF = transformed.select($"label", $"prediction").where("label != prediction")
y1DF.show()
y1DF.count()

transformed.filter("prediction = 0").select("STG", "SCG", "STR", "LPR", "PEG").describe().show()
transformed.filter("prediction = 1").select("STG", "SCG", "STR", "LPR", "PEG").describe().show()

println("No. of mis-matches between predictions and labels =" + y1DF.count()+"\nTotal no. of records=  "+ transformed.count()+"\nCorrect predictions =  "+ (1-(y1DF.count()).toDouble/transformed.count())+"\nMis-match = "+ (y1DF.count()).toDouble/transformed.count())
val testDF = spark.createDataFrame(Seq((0.08,0.08,0.1,0.24,0.9, Vectors.dense(Array(0.08,0.08,0.1,0.24,0.9))))).toDF("STG", "SCG", "STR", "LPR", "PEG", "features")
model.transform(testDF).show()
val testDF = spark.createDataFrame(Seq((0.06,0.06,0.05,0.25,0.33, Vectors.dense(Array(0.06,0.06,0.05,0.25,0.33))))).toDF("STG", "SCG", "STR", "LPR", "PEG", "features")
model.transform(testDF).show()



