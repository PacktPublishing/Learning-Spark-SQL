//Code for Chapter 9 to be executed in Spark shell
//Code for Preprocessing textual data section
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.spark.sql.types._
import scala.util.matching.Regex
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import scala.math
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer, NGram, StopWordsRemover, CountVectorizer}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, IndexToString}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier, LogisticRegression, NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{RegressionEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.clustering.{LDA}
import scala.collection.mutable.WrappedArray
import org.apache.spark.ml._
import org.chap9.edgar10k._

val inputLines = sc.textFile("file:///Users/aurobindosarkar/Downloads/edgardata/0001193125-14-383437.txt")
val linesToString = inputLines.toLocalIterator.mkString
linesToString.length
def deleteAbbrev(instr: String): String = {
      //println("Input string length="+ instr.length())
      val pattern = new Regex("[A-Z]\\.([A-Z]\\.)+")
      val str = pattern.replaceAllIn(instr, " ")
      //println("Output string length ="+ str.length())
      //println("String length reduced by="+ (instr.length - str.length()))
      str
}
val lineRemAbbrev = deleteAbbrev(linesToString)

def deleteDocTypes(instr: String): String = {
      //println("Input string length="+ instr.length())
      val pattern = new Regex("(?s)<TYPE>(GRAPHIC|EXCEL|PDF|ZIP|COVER|CORRESP|EX-10[01].INS|EX-99.SDR [KL].INS|EX-10[01].SCH|EX-99.SDR [KL].SCH|EX-10[01].CAL|EX-99.SDR [KL].CAL|EX-10[01].DEF|EX-99.SDR [KL].LAB|EX-10[01].LAB|EX-99.SDR [KL].LAB|EX-10[01].PRE|EX-99.SDR [KL].PRE|EX-10[01].PRE|EX-99.SDR [KL].PRE).*?</TEXT>")   
      val str = pattern.replaceAllIn(instr, " ")
      //println("Output string length ="+ str.length())
      //println("String length reduced by="+ (instr.length - str.length()))
      str
}
val lineRemDocTypes = deleteDocTypes(lineRemAbbrev)

def deleteMetaData(instr: String): String = {
      val pattern1 = new Regex("<HEAD>.*?</HEAD>")
      val str1 = pattern1.replaceAllIn(instr, " ")
      val pattern2 = new Regex("(?s)<TYPE>.*?<SEQUENCE>.*?<FILENAME>.*?<DESCRIPTION>.*?")
      val str2 = pattern2.replaceAllIn(str1, " ")
      str2
}
val lineRemMetaData = deleteMetaData(lineRemDocTypes)

def deleteTablesNHTMLElem(instr: String): String = {
      val pattern1 = new Regex("(?s)(?i)<Table.*?</Table>")
      val str1 = pattern1.replaceAllIn(instr, " ")
      val pattern2 = new Regex("(?s)<[^>]*>")
      val str2 = pattern2.replaceAllIn(str1, " ")
      str2
}

val lineRemTabNHTML = deleteTablesNHTMLElem(lineRemMetaData)

def deleteExtCharset(instr: String): String = {
      val pattern1 = new Regex("(?s)(&#32;|&nbsp;|&#x(A|a)0;)")
      val str1 = pattern1.replaceAllIn(instr, " ")
      val pattern2 = new Regex("(&#146;|&#x2019;)")
      val str2 = pattern2.replaceAllIn(str1, "'")
      val pattern3 = new Regex("&#120;")
      val str3 = pattern3.replaceAllIn(str2, "x")
      val pattern4 = new Regex("(&#168;|&#167;|&reg;|&#153;|&copy;)")
      val str4 = pattern4.replaceAllIn(str3, " ")
      val pattern5 = new Regex("(&#147;|&#148;|&#x201C;|&#x201D;)")
      val str5 = pattern5.replaceAllIn(str4, "\"")
      val pattern6 = new Regex("&amp;")
      val str6 = pattern6.replaceAllIn(str5, "&")
      val pattern7 = new Regex("(&#150;|&#151;|&#x2013;)")
      val str7 = pattern7.replaceAllIn(str6, "-")
      val pattern8 = new Regex("&#8260;")
      val str8 = pattern8.replaceAllIn(str7, "/")
      str8
}
val lineRemExtChrst = deleteExtCharset(lineRemTabNHTML)

def deleteExcessLFCRWS(instr: String): String = {
      val pattern1 = new Regex("[\n\r]+")
      val str1 = pattern1.replaceAllIn(instr, "\n")
      val pattern2 = new Regex("[\t]+")
      val str2 = pattern2.replaceAllIn(str1, " ")
      val pattern3 = new Regex("\\s+")
      val str3 = pattern3.replaceAllIn(str2, " ")
      str3
}
val lineRemExcessLFCRWS = deleteExcessLFCRWS(lineRemExtChrst)

def deleteStrings(str: String): String = {
      val strings = Array("IDEA: XBRL DOCUMENT", "\\/\\* Do Not Remove This Comment \\*\\/", "v2.4.0.8")
      //println("str="+ str.length())
      var str1 = str
      for(myString <- strings) {
         var pattern1 = new Regex(myString)
         str1 = pattern1.replaceAllIn(str1, " ")
      }
      str1
}
val lineRemStrings = deleteStrings(lineRemExcessLFCRWS)

def deleteAllURLsFileNamesDigitsPunctuationExceptPeriod(instr: String): String = {
      val pattern1 = new Regex("\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]")
      val str1 = pattern1.replaceAllIn(instr, "")
      val pattern2 = new Regex("[_a-zA-Z0-9\\-\\.]+.(txt|sgml|xml|xsd|htm|html)")
      val str2 = pattern2.replaceAllIn(str1, " ")
      val pattern3 = new Regex("[^a-zA-Z|^.]")
      val str3 = pattern3.replaceAllIn(str2, " ")
      str3
}
val lineRemAllUrlsFileNamesDigitsPuncXPeriod = deleteAllURLsFileNamesDigitsPunctuationExceptPeriod(lineRemStrings)

//Code for Computing readability section

val countPeriods = lineRemAllUrlsFileNamesDigitsPuncXPeriod.count(_ == '.')  

def keepOnlyAlphas(instr: String): String = {
      val pattern1 = new Regex("[^a-zA-Z|]")
      val str1 = pattern1.replaceAllIn(instr, " ")
      val str2 = str1.replaceAll("[\\s]+", " ")
      str2
}
val lineWords = keepOnlyAlphas(lineRemAllUrlsFileNamesDigitsPuncXPeriod)

val wordsStringDF = sc.parallelize(List(lineWords)).toDF()
val wordsDF = wordsStringDF.withColumn("words10k", explode(split($"value", "[\\s]"))).drop("value")


val dictDF = spark.read.format("csv").option("header", "true").load("file:///Users/aurobindosarkar/Downloads/edgardata/LoughranMcDonald_MasterDictionary_2014.csv")
val joinWordsDict = wordsDF.join(dictDF, lower(wordsDF("words10k")) === lower(dictDF("Word")))
val numWords = joinWordsDict.count()

val avgWordsPerSentence = numWords / countPeriods
val numPolySylb = joinWordsDict.select("words10k", "Syllables").where(joinWordsDict("Syllables") > 2)
val polySCount = numPolySylb.count()
val fogIndex = 0.4*(avgWordsPerSentence+((polySCount/numWords)*100))

//Proxy for complexity
//File Size for Readability
def calcFileSize(rdd: RDD[String]): Long = {
  rdd.map(_.getBytes("UTF-8").length.toLong)
     .reduce(_+_) //add the sizes together
}
val lines = sc.textFile("file:///Users/aurobindosarkar/Downloads/edgardata/0001193125-14-383437.txt")
val fileSize = calcFileSize(lines)/1000000.0
math.log(fileSize)

//Code for Using word lists section
val negWordCount = joinWordsDict.select("words10k", "negative").where(joinWordsDict("negative") > 0).count()
val sentiment = negWordCount / (numWords.toDouble)
val modalWordCount = joinWordsDict.select("words10k", "modal").where(joinWordsDict("modal") > 0).groupBy("modal").count()
modalWordCount.show()

//Code for Creating data preprocessing pipelines section is partially provided separately.
//Please read the instructions in the book compile into a package.
//Restart Spark shell with the above JAR file and continue executing the following code.

val linesDF1 = sc.textFile("file:///Users/aurobindosarkar/Downloads/reuters21578/reut2-020-1.sgm").toDF()
val tablesNHTMLElemCleaner = new TablesNHTMLElemCleaner().setInputCol("value").setOutputCol("tablesNHTMLElemCleaned")
val allURLsFileNamesDigitsPunctuationExceptPeriodCleaner = new AllURLsFileNamesDigitsPunctuationExceptPeriodCleaner().setInputCol("tablesNHTMLElemCleaned").setOutputCol("allURLsFileNamesDigitsPunctuationExceptPeriodCleaned")
val onlyAlphasCleaner = new OnlyAlphasCleaner().setInputCol("allURLsFileNamesDigitsPunctuationExceptPeriodCleaned").setOutputCol("text")
val excessLFCRWSCleaner = new ExcessLFCRWSCleaner().setInputCol("text").setOutputCol("cleaned")
val tokenizer = new RegexTokenizer().setInputCol("cleaned").setOutputCol("words").setPattern("\\W")
val stopwords: Array[String] = sc.textFile("file:///Users/aurobindosarkar/Downloads/StopWords_GenericLong.txt").flatMap(_.stripMargin.split("\\s+")).collect
val remover = new StopWordsRemover().setStopWords(stopwords).setCaseSensitive(false).setInputCol("words").setOutputCol("filtered")
val pipeline = new Pipeline().setStages(Array(tablesNHTMLElemCleaner, allURLsFileNamesDigitsPunctuationExceptPeriodCleaner, onlyAlphasCleaner, excessLFCRWSCleaner, tokenizer, remover))
val model = pipeline.fit(linesDF1)
val cleanedDF = model.transform(linesDF1).drop("value").drop("tablesNHTMLElemCleaned").drop("excessLFCRWSCleaned").drop("allURLsFileNamesDigitsPunctuationExceptPeriodCleaned").drop("text").drop("word")

val finalDF = cleanedDF.filter(($"cleaned" =!= "") && ($"cleaned" =!= " "))
cleanedDF.count()
val wordsInStoryDF = finalDF.withColumn("wordsInStory", explode(split($"cleaned", "[\\s]"))).drop("cleaned")
val joinWordsDict = wordsInStoryDF.join(dictDF, lower(wordsInStoryDF("wordsInStory")) === lower(dictDF("Word")))
wordsInStoryDF.count()
val numWords = joinWordsDict.count().toDouble
joinWordsDict.select("wordsInStory").show()
val negWordCount = joinWordsDict.select("wordsInStory", "negative").where(joinWordsDict("negative") > 0).count()
val sentiment = negWordCount / (numWords.toDouble)
val modalWordCount = joinWordsDict.select("wordsInStory", "modal").where(joinWordsDict("modal") > 0).groupBy("modal").count()
modalWordCount.show()

val linesDF2 = sc.textFile("file:///Users/aurobindosarkar/Downloads/reuters21578/reut2-008-1.sgm").toDF()
val cleanedDF = model.transform(linesDF2).drop("value").drop("tablesNHTMLElemCleaned").drop("excessLFCRWSCleaned").drop("allURLsFileNamesDigitsPunctuationExceptPeriodCleaned").drop("text").drop("word")

val finalDF = cleanedDF.filter(($"cleaned" =!= "") && ($"cleaned" =!= " "))
cleanedDF.count()
val wordsInStoryDF = finalDF.withColumn("wordsInStory", explode(split($"cleaned", "[\\s]"))).drop("cleaned")
val joinWordsDict = wordsInStoryDF.join(dictDF, lower(wordsInStoryDF("wordsInStory")) === lower(dictDF("Word")))
wordsInStoryDF.count()
val numWords = joinWordsDict.count().toDouble
joinWordsDict.select("wordsInStory").show()
val negWordCount = joinWordsDict.select("wordsInStory", "negative").where(joinWordsDict("negative") > 0).count()
val sentiment = negWordCount / (numWords.toDouble)
val modalWordCount = joinWordsDict.select("wordsInStory", "modal").where(joinWordsDict("modal") > 0).groupBy("modal").count()
modalWordCount.show()

/Code for Understanding themes in document corpuses section
val numTopics: Int = 10
val maxIterations: Int = 100
val vocabSize: Int = 10000

val df = spark.read.format("com.databricks.spark.xml").option("rowTag", "sentences").option("mode", "PERMISSIVE").load("file:///Users/aurobindosarkar/Downloads/corpus/fulltext/*.xml")
val docDF = df.select("sentence._VALUE").withColumn("docId", monotonically_increasing_id()).withColumn("sentences", concat_ws(",", $"_VALUE")).drop("_VALUE")
docDF.show()
// Split each document into words
val tokens = new RegexTokenizer().setGaps(false).setPattern("\\p{L}+").setInputCol("sentences").setOutputCol("words").transform(docDF)
//Remove stop words
val filteredTokens = new StopWordsRemover().setCaseSensitive(false).setInputCol("words").setOutputCol("filtered").transform(tokens)

// Limit to top `vocabSize` most common words and convert to word count vector features
val cvModel = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(vocabSize).fit(filteredTokens)
val termVectors = cvModel.transform(filteredTokens).select("docId", "features")

val lda = new LDA().setK(numTopics).setMaxIter(maxIterations)
val ldaModel = lda.fit(termVectors)
println("Model was fit using parameters: " + ldaModel.parent.extractParamMap)

val ll = ldaModel.logLikelihood(termVectors)
val lp = ldaModel.logPerplexity(termVectors)
println(s"The lower bound on the log likelihood of the entire corpus: $ll")
println(s"The upper bound on perplexity: $lp")

// Describe topics.
val topicsDF = ldaModel.describeTopics(3)
println("The topics described by their top-weighted terms:")
topicsDF.show(false)

val transformed = ldaModel.transform(termVectors)
transformed.select("docId", "topicDistribution").take(3).foreach(println)
val vocab = cvModel.vocabulary

// Shows the result.
for ((row) <- topicsDF) {
        var i = 0
        var termsString = ""
        var topicTermIndicesString = ""
        val topicNumber = row.get(0)
        val topicTerms:WrappedArray[Int]  = row.get(1).asInstanceOf[WrappedArray[Int]]

        for (i <- 0 to topicTerms.length-1){
            topicTermIndicesString += topicTerms(i) +", "
            termsString += vocab(topicTerms(i)) +", "
        }

        println ("Topic: "+ topicNumber+ "|["+topicTermIndicesString + "]|[" + termsString +"]")
}



//Code for Understanding themes in document corpuses section

val inDF = spark.read.json("file:///Users/aurobindosarkar/Downloads/reviews_Electronics_5.json")
inDF.show()
inDF.printSchema()
inDF.groupBy("overall").count().orderBy("overall").show()  
inDF.createOrReplaceTempView("reviewsTable")  
val reviewsDF = spark.sql(  
"""
  SELECT text, label, rowNumber FROM (
    SELECT
       overall AS label, reviewText AS text, row_number() OVER (PARTITION BY overall ORDER BY rand()) AS rowNumber FROM reviewsTable
  ) reviewsTable
  WHERE rowNumber <= 60000
  """
)
reviewsDF.groupBy("label").count().orderBy("label").show()  
val trainingData = reviewsDF.filter(reviewsDF("rowNumber") <= 50000).select("text","label")  
val testData = reviewsDF.filter(reviewsDF("rowNumber") > 10000).select("text","label")
val regexTokenizer = new RegexTokenizer().setPattern("[a-zA-Z']+").setGaps(false).setInputCol("text")
val remover = new StopWordsRemover().setInputCol(regexTokenizer.getOutputCol)
val bigrams = new NGram().setN(2).setInputCol(remover.getOutputCol)
val trigrams = new NGram().setN(3).setInputCol(remover.getOutputCol)
val removerHashingTF = new HashingTF().setInputCol(remover.getOutputCol)
val ngram2HashingTF = new HashingTF().setInputCol(bigrams.getOutputCol)
val ngram3HashingTF = new HashingTF().setInputCol(trigrams.getOutputCol)
val assembler = new VectorAssembler().setInputCols(Array(removerHashingTF.getOutputCol, ngram2HashingTF.getOutputCol, ngram3HashingTF.getOutputCol))
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(reviewsDF)
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
val nb = new NaiveBayes().setLabelCol(labelIndexer.getOutputCol).setFeaturesCol(assembler.getOutputCol).setPredictionCol("prediction").setModelType("multinomial")
val pipeline = new Pipeline().setStages(Array(regexTokenizer, remover, bigrams, trigrams, removerHashingTF, ngram2HashingTF, ngram3HashingTF, assembler, labelIndexer, nb, labelConverter))
val paramGrid = new ParamGridBuilder().addGrid(removerHashingTF.numFeatures, Array(1000,10000)).addGrid(ngram2HashingTF.numFeatures, Array(1000,10000)).addGrid(ngram3HashingTF.numFeatures, Array(1000,10000)).build()
val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")).setEstimatorParamMaps(paramGrid).setNumFolds(5)
val cvModel = cv.fit(trainingData)  
val predictions = cvModel.transform(testData) 
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)  
println("Test Error = " + (1.0 - accuracy))  

//val inDF = spark.read.json("file:///Users/aurobindosarkar/Downloads/reviews_Electronics_5.json")
def udfReviewBins() = udf[Double, Double] { a => val x = a match { case 1.0 => 1.0; case 2.0 => 1.0; case 3.0 => 2.0; case 4.0 => 3.0; case 5.0 => 3.0;}; x;}
val modifiedInDF = inDF.withColumn("rating", udfReviewBins()($"overall")).drop("overall")
modifiedInDF.show()
modifiedInDF.groupBy("rating").count().orderBy("rating").show()  
modifiedInDF.createOrReplaceTempView("modReviewsTable")  
val reviewsDF = spark.sql(  
"""
  SELECT text, label, rowNumber FROM (
    SELECT
       rating AS label, reviewText AS text, row_number() OVER (PARTITION BY rating ORDER BY rand()) AS rowNumber FROM modReviewsTable
  ) modReviewsTable
  WHERE rowNumber <= 120000
  """
)
reviewsDF.groupBy("label").count().orderBy("label").show()  
val trainingData = reviewsDF.filter(reviewsDF("rowNumber") <= 100000).select("text","label")  
val testData = reviewsDF.filter(reviewsDF("rowNumber") > 20000).select("text","label")
val regexTokenizer = new RegexTokenizer().setPattern("[a-zA-Z']+").setGaps(false).setInputCol("text")
val remover = new StopWordsRemover().setInputCol(regexTokenizer.getOutputCol)
val bigrams = new NGram().setN(2).setInputCol(remover.getOutputCol)
val trigrams = new NGram().setN(3).setInputCol(remover.getOutputCol)
val removerHashingTF = new HashingTF().setInputCol(remover.getOutputCol)
val ngram2HashingTF = new HashingTF().setInputCol(bigrams.getOutputCol)
val ngram3HashingTF = new HashingTF().setInputCol(trigrams.getOutputCol)
val assembler = new VectorAssembler().setInputCols(Array(removerHashingTF.getOutputCol, ngram2HashingTF.getOutputCol, ngram3HashingTF.getOutputCol))
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(reviewsDF)
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
val nb = new NaiveBayes().setLabelCol(labelIndexer.getOutputCol).setFeaturesCol(assembler.getOutputCol).setPredictionCol("prediction").setModelType("multinomial")
val pipeline = new Pipeline().setStages(Array(regexTokenizer, remover, bigrams, trigrams, removerHashingTF, ngram2HashingTF, ngram3HashingTF, assembler, labelIndexer, nb, labelConverter))
val paramGrid = new ParamGridBuilder().addGrid(removerHashingTF.numFeatures, Array(1000,10000)).addGrid(ngram2HashingTF.numFeatures, Array(1000,10000)).addGrid(ngram3HashingTF.numFeatures, Array(1000,10000)).build()
val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")).setEstimatorParamMaps(paramGrid).setNumFolds(5)
val cvModel = cv.fit(trainingData)  
val predictions = cvModel.transform(testData) 
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)  
println("Test Error = " + (1.0 - accuracy))  


//Code for Developing a machine learning application section
val inRDD = spark.sparkContext.textFile("file:///Users/aurobindosarkar/Downloads/CNAE-9.data")
val rowRDD = inRDD.map(_.split(",")).map(attributes => Row(attributes(0).toDouble, attributes(1).toDouble, attributes(2).toDouble, attributes(3).toDouble, attributes(4).toDouble, attributes(5).toDouble, attributes(6).toDouble, attributes(7).toDouble, attributes(8).toDouble, attributes(9).toDouble, attributes(10).toDouble,  attributes(11).toDouble, attributes(12).toDouble, attributes(13).toDouble, attributes(14).toDouble, attributes(15).toDouble, attributes(16).toDouble, attributes(17).toDouble, attributes(18).toDouble, attributes(19).toDouble, attributes(20).toDouble,  attributes(21).toDouble, attributes(22).toDouble, attributes(23).toDouble, attributes(24).toDouble, attributes(25).toDouble, attributes(26).toDouble, attributes(27).toDouble, attributes(28).toDouble, attributes(29).toDouble, attributes(30).toDouble, attributes(31).toDouble, attributes(32).toDouble, attributes(33).toDouble, attributes(34).toDouble, attributes(35).toDouble, attributes(36).toDouble, attributes(37).toDouble, attributes(38).toDouble, attributes(39).toDouble, attributes(40).toDouble,  attributes(41).toDouble, attributes(42).toDouble, attributes(43).toDouble, attributes(44).toDouble, attributes(45).toDouble, attributes(46).toDouble, attributes(47).toDouble, attributes(48).toDouble, attributes(49).toDouble, attributes(50).toDouble, attributes(51).toDouble, attributes(52).toDouble, attributes(53).toDouble, attributes(54).toDouble, attributes(55).toDouble, attributes(56).toDouble, attributes(57).toDouble, attributes(58).toDouble, attributes(59).toDouble, attributes(60).toDouble,  attributes(61).toDouble, attributes(62).toDouble, attributes(63).toDouble, attributes(64).toDouble, attributes(65).toDouble, attributes(66).toDouble, attributes(67).toDouble, attributes(68).toDouble, attributes(69).toDouble, attributes(70).toDouble,  attributes(71).toDouble, attributes(72).toDouble, attributes(73).toDouble, attributes(74).toDouble, attributes(75).toDouble, attributes(76).toDouble, attributes(77).toDouble, attributes(78).toDouble, attributes(79).toDouble, attributes(80).toDouble, attributes(81).toDouble, attributes(82).toDouble, attributes(83).toDouble, attributes(84).toDouble, attributes(85).toDouble, attributes(86).toDouble, attributes(87).toDouble, attributes(88).toDouble, attributes(89).toDouble, attributes(90).toDouble,  attributes(91).toDouble, attributes(92).toDouble, attributes(93).toDouble, attributes(94).toDouble, attributes(95).toDouble, attributes(96).toDouble, attributes(97).toDouble, attributes(98).toDouble, attributes(99).toDouble, attributes(100).toDouble, attributes(101).toDouble, attributes(102).toDouble, attributes(103).toDouble, attributes(104).toDouble, attributes(105).toDouble, attributes(106).toDouble, attributes(107).toDouble, attributes(108).toDouble, attributes(109).toDouble, attributes(110).toDouble, attributes(111).toDouble, attributes(112).toDouble, attributes(113).toDouble, attributes(114).toDouble, attributes(115).toDouble, attributes(116).toDouble, attributes(117).toDouble, attributes(118).toDouble, attributes(119).toDouble, attributes(120).toDouble, attributes(121).toDouble, attributes(122).toDouble, attributes(123).toDouble, attributes(124).toDouble, attributes(125).toDouble, attributes(126).toDouble, attributes(127).toDouble, attributes(128).toDouble, attributes(129).toDouble, attributes(130).toDouble, attributes(131).toDouble, attributes(132).toDouble, attributes(133).toDouble, attributes(134).toDouble, attributes(135).toDouble, attributes(136).toDouble, attributes(137).toDouble, attributes(138).toDouble, attributes(139).toDouble, attributes(140).toDouble, attributes(141).toDouble, attributes(142).toDouble, attributes(143).toDouble, attributes(144).toDouble, attributes(145).toDouble, attributes(146).toDouble, attributes(147).toDouble, attributes(148).toDouble, attributes(149).toDouble, attributes(150).toDouble, attributes(151).toDouble, attributes(152).toDouble, attributes(153).toDouble, attributes(154).toDouble, attributes(155).toDouble, attributes(156).toDouble, attributes(157).toDouble, attributes(158).toDouble, attributes(159).toDouble, attributes(160).toDouble, attributes(161).toDouble, attributes(162).toDouble, attributes(163).toDouble, attributes(164).toDouble, attributes(165).toDouble, attributes(166).toDouble, attributes(167).toDouble, attributes(168).toDouble, attributes(169).toDouble, attributes(170).toDouble, attributes(171).toDouble, attributes(172).toDouble, attributes(173).toDouble, attributes(174).toDouble, attributes(175).toDouble, attributes(176).toDouble, attributes(177).toDouble, attributes(178).toDouble, attributes(179).toDouble, attributes(180).toDouble, attributes(181).toDouble, attributes(182).toDouble, attributes(183).toDouble, attributes(184).toDouble, attributes(185).toDouble, attributes(186).toDouble, attributes(187).toDouble, attributes(188).toDouble, attributes(189).toDouble, attributes(190).toDouble, attributes(191).toDouble, attributes(192).toDouble, attributes(193).toDouble, attributes(194).toDouble, attributes(195).toDouble, attributes(196).toDouble, attributes(197).toDouble, attributes(198).toDouble, attributes(199).toDouble, attributes(200).toDouble, attributes(201).toDouble, attributes(202).toDouble, attributes(203).toDouble, attributes(204).toDouble, attributes(205).toDouble, attributes(206).toDouble, attributes(207).toDouble, attributes(208).toDouble, attributes(209).toDouble, attributes(210).toDouble, attributes(211).toDouble, attributes(212).toDouble, attributes(213).toDouble, attributes(214).toDouble, attributes(215).toDouble, attributes(216).toDouble, attributes(217).toDouble, attributes(218).toDouble, attributes(219).toDouble, attributes(220).toDouble,attributes(221).toDouble, attributes(222).toDouble, attributes(223).toDouble, attributes(224).toDouble, attributes(225).toDouble, attributes(226).toDouble, attributes(227).toDouble, attributes(228).toDouble, attributes(229).toDouble, attributes(230).toDouble, attributes(231).toDouble, attributes(232).toDouble, attributes(233).toDouble, attributes(234).toDouble, attributes(235).toDouble, attributes(236).toDouble, attributes(237).toDouble, attributes(238).toDouble, attributes(239).toDouble, attributes(240).toDouble, attributes(241).toDouble, attributes(242).toDouble, attributes(243).toDouble, attributes(244).toDouble, attributes(245).toDouble, attributes(246).toDouble, attributes(247).toDouble, attributes(248).toDouble, attributes(249).toDouble, attributes(250).toDouble, attributes(251).toDouble, attributes(252).toDouble, attributes(253).toDouble, attributes(254).toDouble, attributes(255).toDouble, attributes(256).toDouble, attributes(257).toDouble, attributes(258).toDouble, attributes(259).toDouble, attributes(260).toDouble, attributes(261).toDouble, attributes(262).toDouble, attributes(263).toDouble, attributes(264).toDouble, attributes(265).toDouble, attributes(266).toDouble, attributes(267).toDouble, attributes(268).toDouble, attributes(269).toDouble, attributes(270).toDouble, attributes(271).toDouble, attributes(272).toDouble, attributes(273).toDouble, attributes(274).toDouble, attributes(275).toDouble, attributes(276).toDouble, attributes(277).toDouble, attributes(278).toDouble, attributes(279).toDouble, attributes(280).toDouble, attributes(281).toDouble, attributes(282).toDouble, attributes(283).toDouble, attributes(284).toDouble, attributes(285).toDouble, attributes(286).toDouble, attributes(287).toDouble, attributes(288).toDouble, attributes(289).toDouble, attributes(290).toDouble, attributes(291).toDouble, attributes(292).toDouble, attributes(293).toDouble, attributes(294).toDouble, attributes(295).toDouble, attributes(296).toDouble, attributes(297).toDouble, attributes(298).toDouble, attributes(299).toDouble, attributes(300).toDouble, attributes(301).toDouble, attributes(302).toDouble, attributes(303).toDouble, attributes(304).toDouble, attributes(305).toDouble, attributes(306).toDouble, attributes(307).toDouble, attributes(308).toDouble, attributes(309).toDouble, attributes(310).toDouble, attributes(311).toDouble, attributes(312).toDouble, attributes(313).toDouble, attributes(314).toDouble, attributes(315).toDouble, attributes(316).toDouble, attributes(317).toDouble, attributes(318).toDouble, attributes(319).toDouble, attributes(320).toDouble, attributes(321).toDouble, attributes(322).toDouble, attributes(323).toDouble, attributes(324).toDouble, attributes(325).toDouble, attributes(326).toDouble, attributes(327).toDouble, attributes(328).toDouble, attributes(329).toDouble, attributes(330).toDouble, attributes(331).toDouble, attributes(332).toDouble, attributes(333).toDouble, attributes(334).toDouble, attributes(335).toDouble, attributes(336).toDouble, attributes(337).toDouble, attributes(338).toDouble, attributes(339).toDouble, attributes(340).toDouble, attributes(341).toDouble, attributes(342).toDouble, attributes(343).toDouble, attributes(344).toDouble, attributes(345).toDouble, attributes(346).toDouble, attributes(347).toDouble, attributes(348).toDouble, attributes(349).toDouble, attributes(350).toDouble, attributes(351).toDouble, attributes(352).toDouble, attributes(353).toDouble, attributes(354).toDouble, attributes(355).toDouble, attributes(356).toDouble, attributes(357).toDouble, attributes(358).toDouble, attributes(359).toDouble, attributes(360).toDouble, attributes(361).toDouble, attributes(362).toDouble, attributes(363).toDouble, attributes(364).toDouble, attributes(365).toDouble, attributes(366).toDouble, attributes(367).toDouble, attributes(368).toDouble, attributes(369).toDouble, attributes(370).toDouble, attributes(371).toDouble, attributes(372).toDouble, attributes(373).toDouble, attributes(374).toDouble, attributes(375).toDouble, attributes(376).toDouble, attributes(377).toDouble, attributes(378).toDouble, attributes(379).toDouble, attributes(380).toDouble, attributes(381).toDouble, attributes(382).toDouble, attributes(383).toDouble, attributes(384).toDouble, attributes(385).toDouble, attributes(386).toDouble, attributes(387).toDouble, attributes(388).toDouble, attributes(389).toDouble, attributes(390).toDouble, attributes(391).toDouble, attributes(392).toDouble, attributes(393).toDouble, attributes(394).toDouble, attributes(395).toDouble, attributes(396).toDouble, attributes(397).toDouble, attributes(398).toDouble, attributes(399).toDouble, attributes(400).toDouble, attributes(401).toDouble, attributes(402).toDouble, attributes(403).toDouble, attributes(404).toDouble, attributes(405).toDouble, attributes(406).toDouble, attributes(407).toDouble, attributes(408).toDouble, attributes(409).toDouble, attributes(410).toDouble, attributes(411).toDouble, attributes(412).toDouble, attributes(413).toDouble, attributes(414).toDouble, attributes(415).toDouble, attributes(416).toDouble, attributes(417).toDouble, attributes(418).toDouble, attributes(419).toDouble, attributes(420).toDouble, attributes(421).toDouble, attributes(422).toDouble, attributes(423).toDouble, attributes(424).toDouble, attributes(425).toDouble, attributes(426).toDouble, attributes(427).toDouble, attributes(428).toDouble, attributes(429).toDouble, attributes(430).toDouble, attributes(431).toDouble, attributes(432).toDouble, attributes(433).toDouble, attributes(434).toDouble, attributes(435).toDouble, attributes(436).toDouble, attributes(437).toDouble, attributes(438).toDouble, attributes(439).toDouble, attributes(440).toDouble, attributes(441).toDouble, attributes(442).toDouble, attributes(443).toDouble, attributes(444).toDouble, attributes(445).toDouble, attributes(446).toDouble, attributes(447).toDouble, attributes(448).toDouble, attributes(449).toDouble, attributes(450).toDouble, attributes(451).toDouble, attributes(452).toDouble, attributes(453).toDouble, attributes(454).toDouble, attributes(455).toDouble, attributes(456).toDouble, attributes(457).toDouble, attributes(458).toDouble, attributes(459).toDouble, attributes(460).toDouble, attributes(461).toDouble, attributes(462).toDouble, attributes(463).toDouble, attributes(464).toDouble, attributes(465).toDouble, attributes(466).toDouble, attributes(467).toDouble, attributes(468).toDouble, attributes(469).toDouble, attributes(470).toDouble, attributes(471).toDouble, attributes(472).toDouble, attributes(473).toDouble, attributes(474).toDouble, attributes(475).toDouble, attributes(476).toDouble, attributes(477).toDouble, attributes(478).toDouble, attributes(479).toDouble, attributes(480).toDouble, attributes(481).toDouble, attributes(482).toDouble, attributes(483).toDouble, attributes(484).toDouble, attributes(485).toDouble, attributes(486).toDouble, attributes(487).toDouble, attributes(488).toDouble, attributes(489).toDouble, attributes(490).toDouble, attributes(491).toDouble, attributes(492).toDouble, attributes(493).toDouble, attributes(494).toDouble, attributes(495).toDouble, attributes(496).toDouble, attributes(497).toDouble, attributes(498).toDouble, attributes(499).toDouble, attributes(400).toDouble, attributes(501).toDouble, attributes(502).toDouble, attributes(503).toDouble, attributes(504).toDouble, attributes(505).toDouble, attributes(506).toDouble, attributes(507).toDouble, attributes(508).toDouble, attributes(509).toDouble, attributes(510).toDouble, attributes(511).toDouble, attributes(512).toDouble, attributes(513).toDouble, attributes(514).toDouble, attributes(515).toDouble, attributes(516).toDouble, attributes(517).toDouble, attributes(518).toDouble, attributes(519).toDouble, attributes(520).toDouble, attributes(521).toDouble, attributes(522).toDouble, attributes(523).toDouble, attributes(524).toDouble, attributes(525).toDouble, attributes(526).toDouble, attributes(527).toDouble, attributes(528).toDouble, attributes(529).toDouble, attributes(530).toDouble, attributes(531).toDouble, attributes(532).toDouble, attributes(533).toDouble, attributes(534).toDouble, attributes(535).toDouble, attributes(536).toDouble, attributes(537).toDouble, attributes(538).toDouble, attributes(539).toDouble, attributes(540).toDouble, attributes(541).toDouble, attributes(542).toDouble, attributes(543).toDouble, attributes(544).toDouble, attributes(545).toDouble, attributes(546).toDouble, attributes(547).toDouble, attributes(548).toDouble, attributes(549).toDouble, attributes(550).toDouble, attributes(551).toDouble, attributes(552).toDouble, attributes(553).toDouble, attributes(554).toDouble, attributes(555).toDouble, attributes(556).toDouble, attributes(557).toDouble, attributes(558).toDouble, attributes(559).toDouble, attributes(560).toDouble, attributes(561).toDouble, attributes(562).toDouble, attributes(563).toDouble, attributes(564).toDouble, attributes(565).toDouble, attributes(566).toDouble, attributes(567).toDouble, attributes(568).toDouble, attributes(569).toDouble, attributes(570).toDouble, attributes(571).toDouble, attributes(572).toDouble, attributes(573).toDouble, attributes(574).toDouble, attributes(575).toDouble, attributes(576).toDouble, attributes(577).toDouble, attributes(578).toDouble, attributes(579).toDouble, attributes(580).toDouble, attributes(581).toDouble, attributes(582).toDouble, attributes(583).toDouble, attributes(584).toDouble, attributes(585).toDouble, attributes(586).toDouble, attributes(587).toDouble, attributes(588).toDouble, attributes(589).toDouble, attributes(590).toDouble, attributes(591).toDouble, attributes(592).toDouble, attributes(593).toDouble, attributes(594).toDouble, attributes(595).toDouble, attributes(596).toDouble, attributes(597).toDouble, attributes(598).toDouble, attributes(599).toDouble, attributes(600).toDouble, attributes(601).toDouble, attributes(602).toDouble, attributes(603).toDouble, attributes(604).toDouble, attributes(605).toDouble, attributes(606).toDouble, attributes(607).toDouble, attributes(608).toDouble, attributes(609).toDouble, attributes(610).toDouble, attributes(611).toDouble, attributes(612).toDouble, attributes(613).toDouble, attributes(614).toDouble, attributes(615).toDouble, attributes(616).toDouble, attributes(617).toDouble, attributes(618).toDouble, attributes(619).toDouble, attributes(620).toDouble, attributes(621).toDouble, attributes(622).toDouble, attributes(623).toDouble, attributes(624).toDouble, attributes(625).toDouble, attributes(626).toDouble, attributes(627).toDouble, attributes(628).toDouble, attributes(629).toDouble, attributes(630).toDouble, attributes(631).toDouble, attributes(632).toDouble, attributes(633).toDouble, attributes(634).toDouble, attributes(635).toDouble, attributes(636).toDouble, attributes(637).toDouble, attributes(638).toDouble, attributes(639).toDouble, attributes(640).toDouble, attributes(641).toDouble, attributes(642).toDouble, attributes(643).toDouble, attributes(644).toDouble, attributes(645).toDouble, attributes(646).toDouble, attributes(647).toDouble, attributes(648).toDouble, attributes(649).toDouble, attributes(650).toDouble, attributes(651).toDouble, attributes(652).toDouble, attributes(653).toDouble, attributes(654).toDouble, attributes(655).toDouble, attributes(656).toDouble, attributes(657).toDouble, attributes(658).toDouble, attributes(659).toDouble, attributes(660).toDouble, attributes(661).toDouble, attributes(662).toDouble, attributes(663).toDouble, attributes(664).toDouble, attributes(665).toDouble, attributes(666).toDouble, attributes(667).toDouble, attributes(668).toDouble, attributes(669).toDouble, attributes(670).toDouble, attributes(671).toDouble, attributes(672).toDouble, attributes(673).toDouble, attributes(674).toDouble, attributes(675).toDouble, attributes(676).toDouble, attributes(677).toDouble, attributes(678).toDouble, attributes(679).toDouble, attributes(680).toDouble, attributes(681).toDouble, attributes(682).toDouble, attributes(683).toDouble, attributes(684).toDouble, attributes(685).toDouble, attributes(686).toDouble, attributes(687).toDouble, attributes(688).toDouble, attributes(689).toDouble, attributes(690).toDouble, attributes(691).toDouble, attributes(692).toDouble, attributes(693).toDouble, attributes(694).toDouble, attributes(695).toDouble, attributes(696).toDouble, attributes(697).toDouble, attributes(698).toDouble, attributes(699).toDouble, attributes(700).toDouble, attributes(701).toDouble, attributes(702).toDouble, attributes(703).toDouble, attributes(704).toDouble, attributes(705).toDouble, attributes(706).toDouble, attributes(707).toDouble, attributes(708).toDouble, attributes(709).toDouble, attributes(710).toDouble, attributes(711).toDouble, attributes(712).toDouble, attributes(713).toDouble, attributes(714).toDouble, attributes(715).toDouble, attributes(716).toDouble, attributes(717).toDouble, attributes(718).toDouble, attributes(719).toDouble, attributes(720).toDouble, attributes(721).toDouble, attributes(722).toDouble, attributes(723).toDouble, attributes(724).toDouble, attributes(725).toDouble, attributes(726).toDouble, attributes(727).toDouble, attributes(728).toDouble, attributes(729).toDouble, attributes(730).toDouble, attributes(731).toDouble, attributes(732).toDouble, attributes(733).toDouble, attributes(734).toDouble, attributes(735).toDouble, attributes(736).toDouble, attributes(737).toDouble, attributes(738).toDouble, attributes(739).toDouble, attributes(740).toDouble, attributes(741).toDouble, attributes(742).toDouble, attributes(743).toDouble, attributes(744).toDouble, attributes(745).toDouble, attributes(746).toDouble, attributes(747).toDouble, attributes(748).toDouble, attributes(749).toDouble, attributes(750).toDouble, attributes(751).toDouble, attributes(752).toDouble, attributes(753).toDouble, attributes(754).toDouble, attributes(755).toDouble, attributes(756).toDouble, attributes(757).toDouble, attributes(758).toDouble, attributes(759).toDouble, attributes(760).toDouble, attributes(761).toDouble, attributes(762).toDouble, attributes(763).toDouble, attributes(764).toDouble, attributes(765).toDouble, attributes(766).toDouble, attributes(767).toDouble, attributes(768).toDouble, attributes(769).toDouble, attributes(770).toDouble, attributes(771).toDouble, attributes(772).toDouble, attributes(773).toDouble, attributes(774).toDouble, attributes(775).toDouble, attributes(776).toDouble, attributes(777).toDouble, attributes(778).toDouble, attributes(779).toDouble, attributes(780).toDouble, attributes(781).toDouble, attributes(782).toDouble, attributes(783).toDouble, attributes(784).toDouble, attributes(785).toDouble, attributes(786).toDouble, attributes(787).toDouble, attributes(788).toDouble, attributes(789).toDouble, attributes(790).toDouble, attributes(791).toDouble, attributes(792).toDouble, attributes(793).toDouble, attributes(794).toDouble, attributes(795).toDouble, attributes(796).toDouble, attributes(797).toDouble, attributes(798).toDouble, attributes(799).toDouble, attributes(800).toDouble, attributes(801).toDouble, attributes(802).toDouble, attributes(803).toDouble, attributes(804).toDouble, attributes(805).toDouble, attributes(806).toDouble, attributes(807).toDouble, attributes(808).toDouble, attributes(809).toDouble, attributes(810).toDouble, attributes(811).toDouble, attributes(812).toDouble, attributes(813).toDouble, attributes(814).toDouble, attributes(815).toDouble, attributes(816).toDouble, attributes(817).toDouble, attributes(818).toDouble, attributes(819).toDouble, attributes(820).toDouble, attributes(821).toDouble, attributes(822).toDouble, attributes(823).toDouble, attributes(824).toDouble, attributes(825).toDouble, attributes(826).toDouble, attributes(827).toDouble, attributes(828).toDouble, attributes(829).toDouble, attributes(830).toDouble, attributes(831).toDouble, attributes(832).toDouble, attributes(833).toDouble, attributes(834).toDouble, attributes(835).toDouble, attributes(836).toDouble, attributes(837).toDouble, attributes(838).toDouble, attributes(839).toDouble, attributes(840).toDouble, attributes(841).toDouble, attributes(842).toDouble, attributes(843).toDouble, attributes(844).toDouble, attributes(845).toDouble, attributes(846).toDouble, attributes(847).toDouble, attributes(848).toDouble, attributes(849).toDouble, attributes(850).toDouble, attributes(851).toDouble, attributes(852).toDouble, attributes(853).toDouble, attributes(854).toDouble, attributes(855).toDouble, attributes(856).toDouble))

val schemaString = "label _c715 _c195 _c480 _c856 _c136 _c53 _c429 _c732 _c271 _c742 _c172 _c45 _c374 _c233 _c720 _c294 _c461 _c87 _c599 _c84 _c28 _c79 _c615 _c243 _c603 _c531 _c503 _c630 _c33 _c428 _c385 _c751 _c664 _c540 _c626 _c730 _c9 _c699 _c117 _c1 _c49 _c235 _c712 _c753 _c12 _c342 _c64 _c460 _c529 _c608 _c375 _c491 _c373 _c489 _c815 _c242 _c706 _c616 _c334 _c305 _c191 _c789 _c539 _c96 _c144 _c506 _c29 _c345 _c298 _c26 _c468 _c292 _c118 _c283 _c262 _c398 _c709 _c55 _c571 _c404 _c672 _c43 _c107 _c729 _c302 _c219 _c138 _c484 _c90 _c289 _c535 _c502 _c451 _c19 _c258 _c146 _c538 _c30 _c774 _c140 _c152 _c559 _c93 _c14 _c632 _c687 _c708 _c800 _c72 _c634 _c130 _c206 _c701 _c102 _c524 _c150 _c196 _c313 _c510 _c157 _c76 _c749 _c351 _c660 _c847 _c446 _c310 _c482 _c32 _c255 _c377 _c764 _c161 _c34 _c629 _c809 _c81 _c685 _c519 _c739 _c771 _c396 _c432 _c547 _c845 _c591 _c678 _c368 _c434 _c222 _c98 _c214 _c307 _c683 _c363 _c279 _c240 _c509 _c347 _c736 _c601 _c301 _c62 _c458 _c330 _c80 _c513 _c740 _c60 _c763 _c602 _c777 _c248 _c745 _c611 _c269 _c167 _c77 _c200 _c360 _c614 _c238 _c698 _c265 _c114 _c795 _c576 _c367 _c580 _c570 _c661 _c253 _c209 _c163 _c833 _c834 _c177 _c518 _c563 _c558 _c438 _c761 _c476 _c454 _c188 _c588 _c623 _c416 _c324 _c399 _c779 _c666 _c528 _c606 _c202 _c743 _c5 _c103 _c646 _c449 _c74 _c694 _c572 _c251 _c818 _c171 _c315 _c541 _c554 _c525 _c67 _c773 _c663 _c293 _c193 _c376 _c713 _c88 _c231 _c223 _c854 _c762 _c598 _c22 _c94 _c831 _c314 _c387 _c155 _c704 _c422 _c421 _c389 _c433 _c397 _c309 _c217 _c132 _c317 _c752 _c597 _c331 _c741 _c798 _c840 _c165 _c569 _c494 _c671 _c544 _c154 _c278 _c718 _c796 _c384 _c178 _c562 _c261 _c241 _c371 _c552 _c500 _c748 _c583 _c284 _c220 _c556 _c252 _c51 _c272 _c42 _c391 _c393 _c205 _c276 _c512 _c520 _c275 _c97 _c785 _c182 _c530 _c333 _c670 _c369 _c492 _c784 _c166 _c453 _c450 _c325 _c483 _c2 _c504 _c221 _c224 _c287 _c575 _c759 _c553 _c813 _c125 _c605 _c662 _c142 _c651 _c18 _c781 _c697 _c320 _c365 _c101 _c493 _c70 _c668 _c405 _c545 _c568 _c120 _c10 _c92 _c216 _c424 _c439 _c357 _c122 _c808 _c73 _c498 _c25 _c329 _c207 _c585 _c719 _c227 _c382 _c536 _c587 _c91 _c744 _c3 _c417 _c769 _c378 _c505 _c133 _c782 _c444 _c411 _c543 _c430 _c147 _c848 _c643 _c372 _c791 _c778 _c822 _c464 _c31 _c335 _c738 _c722 _c406 _c185 _c338 _c499 _c682 _c199 _c158 _c705 _c249 _c112 _c415 _c457 _c515 _c792 _c680 _c431 _c497 _c823 _c40 _c485 _c814 _c472 _c638 _c264 _c443 _c296 _c770 _c534 _c747 _c838 _c27 _c452 _c578 _c304 _c711 _c11 _c825 _c827 _c319 _c175 _c280 _c37 _c794 _c356 _c479 _c236 _c700 _c48 _c566 _c364 _c573 _c819 _c669 _c691 _c100 _c690 _c478 _c440 _c38 _c637 _c650 _c725 _c564 _c187 _c737 _c225 _c256 _c627 _c143 _c703 _c625 _c192 _c213 _c581 _c400 _c490 _c99 _c806 _c409 _c151 _c341 _c622 _c39 _c212 _c788 _c148 _c127 _c514 _c282 _c675 _c134 _c724 _c288 _c13 _c731 _c633 _c710 _c390 _c567 _c448 _c113 _c803 _c54 _c620 _c83 _c590 _c674 _c297 _c410 _c106 _c46 _c226 _c198 _c322 _c734 _c735 _c352 _c702 _c760 _c247 _c557 _c496 _c169 _c162 _c201 _c842 _c550 _c647 _c714 _c487 _c618 _c291 _c816 _c481 _c402 _c855 _c328 _c156 _c343 _c582 _c641 _c655 _c71 _c149 _c435 _c802 _c332 _c665 _c386 _c350 _c756 _c758 _c786 _c418 _c17 _c139 _c594 _c244 _c395 _c436 _c561 _c23 _c15 _c7 _c47 _c635 _c797 _c190 _c686 _c467 _c645 _c750 _c66 _c116 _c111 _c624 _c810 _c654 _c86 _c281 _c366 _c488 _c78 _c308 _c412 _c303 _c394 _c447 _c846 _c257 _c679 _c85 _c640 _c639 _c59 _c517 _c592 _c837 _c392 _c380 _c613 _c548 _c370 _c754 _c852 _c63 _c723 _c473 _c746 _c466 _c16 _c526 _c780 _c673 _c124 _c230 _c521 _c323 _c839 _c765 _c316 _c50 _c344 _c853 _c312 _c89 _c237 _c805 _c383 _c354 _c649 _c180 _c652 _c75 _c104 _c246 _c442 _c596 _c542 _c4 _c607 _c425 _c153 _c648 _c145 _c427 _c170 _c565 _c717 _c455 _c35 _c628 _c757 _c95 _c726 _c653 _c239 _c437 _c218 _c696 _c131 _c326 _c110 _c695 _c300 _c844 _c277 _c259 _c407 _c318 _c688 _c204 _c129 _c527 _c426 _c811 _c56 _c290 _c692 _c612 _c456 _c677 _c24 _c203 _c337 _c707 _c105 _c600 _c656 _c667 _c812 _c574 _c208 _c168 _c340 _c551 _c128 _c511 _c267 _c589 _c108 _c609 _c44 _c232 _c619 _c659 _c642 _c68 _c768 _c229 _c194 _c801 _c41 _c254 _c173 _c776 _c721 _c228 _c353 _c109 _c830 _c824 _c465 _c537 _c804 _c285 _c459 _c189 _c61 _c266 _c462 _c348 _c441 _c174 _c577 _c463 _c210 _c184 _c6 _c362 _c359 _c826 _c508 _c65 _c579 _c523 _c20 _c388 _c186 _c270 _c414 _c477 _c164 _c8 _c644 _c775 _c817 _c584 _c546 _c401 _c121 _c260 _c197 _c115 _c250 _c790 _c828 _c58 _c346 _c413 _c681 _c843 _c851 _c234 _c268 _c295 _c381 _c379 _c727 _c273 _c684 _c245 _c475 _c631 _c311 _c183 _c420 _c126 _c532 _c617 _c658 _c850 _c471 _c835 _c181 _c767 _c358 _c327 _c469 _c445 _c211 _c215 _c176 _c593 _c610 _c560 _c486 _c339 _c586 _c274 _c403 _c793 _c355 _c507 _c57 _c733 _c159 _c321 _c836 _c501 _c533 _c716 _c820 _c849 _c783 _c832 _c787 _c595 _c179 _c689 _c21 _c821 _c657 _c306 _c807 _c349 _c408 _c604 _c766 _c676 _c52 _c755 _c728 _c693 _c119 _c160 _c141 _c516 _c419 _c69 _c621 _c423 _c137 _c549 _c636 _c772 _c799 _c336 _c841 _c82 _c123 _c474 _c470 _c286 _c555 _c36 _c299 _c829 _c361 _c263 _c522 _c495 _c135"
val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, DoubleType, nullable = false))
val schema = StructType(fields)
val inDF = spark.createDataFrame(rowRDD, schema)
inDF.take(1).foreach(println)

val indexedDF= inDF.withColumn("id", monotonically_increasing_id())
indexedDF.select("label", "id").show()
val columnNames = Array("_c715","_c195","_c480","_c856","_c136","_c53","_c429","_c732","_c271","_c742","_c172","_c45","_c374","_c233","_c720","_c294","_c461","_c87","_c599","_c84","_c28","_c79","_c615","_c243","_c603","_c531","_c503","_c630","_c33","_c428","_c385","_c751","_c664","_c540","_c626","_c730","_c9","_c699","_c117","_c1","_c49","_c235","_c712","_c753","_c12","_c342","_c64","_c460","_c529","_c608","_c375","_c491","_c373","_c489","_c815","_c242","_c706","_c616","_c334","_c305","_c191","_c789","_c539","_c96","_c144","_c506","_c29","_c345","_c298","_c26","_c468","_c292","_c118","_c283","_c262","_c398","_c709","_c55","_c571","_c404","_c672","_c43","_c107","_c729","_c302","_c219","_c138","_c484","_c90","_c289","_c535","_c502","_c451","_c19","_c258","_c146","_c538","_c30","_c774","_c140","_c152","_c559","_c93","_c14","_c632","_c687","_c708","_c800","_c72","_c634","_c130","_c206","_c701","_c102","_c524","_c150","_c196","_c313","_c510","_c157","_c76","_c749","_c351","_c660","_c847","_c446","_c310","_c482","_c32","_c255","_c377","_c764","_c161","_c34","_c629","_c809","_c81","_c685","_c519","_c739","_c771","_c396","_c432","_c547","_c845","_c591","_c678","_c368","_c434","_c222","_c98","_c214","_c307","_c683","_c363","_c279","_c240","_c509","_c347","_c736","_c601","_c301","_c62","_c458","_c330","_c80","_c513","_c740","_c60","_c763","_c602","_c777","_c248","_c745","_c611","_c269","_c167","_c77","_c200","_c360","_c614","_c238","_c698","_c265","_c114","_c795","_c576","_c367","_c580","_c570","_c661","_c253","_c209","_c163","_c833","_c834","_c177","_c518","_c563","_c558","_c438","_c761","_c476","_c454","_c188","_c588","_c623","_c416","_c324","_c399","_c779","_c666","_c528","_c606","_c202","_c743","_c5","_c103","_c646","_c449","_c74","_c694","_c572","_c251","_c818","_c171","_c315","_c541","_c554","_c525","_c67","_c773","_c663","_c293","_c193","_c376","_c713","_c88","_c231","_c223","_c854","_c762","_c598","_c22","_c94","_c831","_c314","_c387","_c155","_c704","_c422","_c421","_c389","_c433","_c397","_c309","_c217","_c132","_c317","_c752","_c597","_c331","_c741","_c798","_c840","_c165","_c569","_c494","_c671","_c544","_c154","_c278","_c718","_c796","_c384","_c178","_c562","_c261","_c241","_c371","_c552","_c500","_c748","_c583","_c284","_c220","_c556","_c252","_c51","_c272","_c42","_c391","_c393","_c205","_c276","_c512","_c520","_c275","_c97","_c785","_c182","_c530","_c333","_c670","_c369","_c492","_c784","_c166","_c453","_c450","_c325","_c483","_c2","_c504","_c221","_c224","_c287","_c575","_c759","_c553","_c813","_c125","_c605","_c662","_c142","_c651","_c18","_c781","_c697","_c320","_c365","_c101","_c493","_c70","_c668","_c405","_c545","_c568","_c120","_c10","_c92","_c216","_c424","_c439","_c357","_c122","_c808","_c73","_c498","_c25","_c329","_c207","_c585","_c719","_c227","_c382","_c536","_c587","_c91","_c744","_c3","_c417","_c769","_c378","_c505","_c133","_c782","_c444","_c411","_c543","_c430","_c147","_c848","_c643","_c372","_c791","_c778","_c822","_c464","_c31","_c335","_c738","_c722","_c406","_c185","_c338","_c499","_c682","_c199","_c158","_c705","_c249","_c112","_c415","_c457","_c515","_c792","_c680","_c431","_c497","_c823","_c40","_c485","_c814","_c472","_c638","_c264","_c443","_c296","_c770","_c534","_c747","_c838","_c27","_c452","_c578","_c304","_c711","_c11","_c825","_c827","_c319","_c175","_c280","_c37","_c794","_c356","_c479","_c236","_c700","_c48","_c566","_c364","_c573","_c819","_c669","_c691","_c100","_c690","_c478","_c440","_c38","_c637","_c650","_c725","_c564","_c187","_c737","_c225","_c256","_c627","_c143","_c703","_c625","_c192","_c213","_c581","_c400","_c490","_c99","_c806","_c409","_c151","_c341","_c622","_c39","_c212","_c788","_c148","_c127","_c514","_c282","_c675","_c134","_c724","_c288","_c13","_c731","_c633","_c710","_c390","_c567","_c448","_c113","_c803","_c54","_c620","_c83","_c590","_c674","_c297","_c410","_c106","_c46","_c226","_c198","_c322","_c734","_c735","_c352","_c702","_c760","_c247","_c557","_c496","_c169","_c162","_c201","_c842","_c550","_c647","_c714","_c487","_c618","_c291","_c816","_c481","_c402","_c855","_c328","_c156","_c343","_c582","_c641","_c655","_c71","_c149","_c435","_c802","_c332","_c665","_c386","_c350","_c756","_c758","_c786","_c418","_c17","_c139","_c594","_c244","_c395","_c436","_c561","_c23","_c15","_c7","_c47","_c635","_c797","_c190","_c686","_c467","_c645","_c750","_c66","_c116","_c111","_c624","_c810","_c654","_c86","_c281","_c366","_c488","_c78","_c308","_c412","_c303","_c394","_c447","_c846","_c257","_c679","_c85","_c640","_c639","_c59","_c517","_c592","_c837","_c392","_c380","_c613","_c548","_c370","_c754","_c852","_c63","_c723","_c473","_c746","_c466","_c16","_c526","_c780","_c673","_c124","_c230","_c521","_c323","_c839","_c765","_c316","_c50","_c344","_c853","_c312","_c89","_c237","_c805","_c383","_c354","_c649","_c180","_c652","_c75","_c104","_c246","_c442","_c596","_c542","_c4","_c607","_c425","_c153","_c648","_c145","_c427","_c170","_c565","_c717","_c455","_c35","_c628","_c757","_c95","_c726","_c653","_c239","_c437","_c218","_c696","_c131","_c326","_c110","_c695","_c300","_c844","_c277","_c259","_c407","_c318","_c688","_c204","_c129","_c527","_c426","_c811","_c56","_c290","_c692","_c612","_c456","_c677","_c24","_c203","_c337","_c707","_c105","_c600","_c656","_c667","_c812","_c574","_c208","_c168","_c340","_c551","_c128","_c511","_c267","_c589","_c108","_c609","_c44","_c232","_c619","_c659","_c642","_c68","_c768","_c229","_c194","_c801","_c41","_c254","_c173","_c776","_c721","_c228","_c353","_c109","_c830","_c824","_c465","_c537","_c804","_c285","_c459","_c189","_c61","_c266","_c462","_c348","_c441","_c174","_c577","_c463","_c210","_c184","_c6","_c362","_c359","_c826","_c508","_c65","_c579","_c523","_c20","_c388","_c186","_c270","_c414","_c477","_c164","_c8","_c644","_c775","_c817","_c584","_c546","_c401","_c121","_c260","_c197","_c115","_c250","_c790","_c828","_c58","_c346","_c413","_c681","_c843","_c851","_c234","_c268","_c295","_c381","_c379","_c727","_c273","_c684","_c245","_c475","_c631","_c311","_c183","_c420","_c126","_c532","_c617","_c658","_c850","_c471","_c835","_c181","_c767","_c358","_c327","_c469","_c445","_c211","_c215","_c176","_c593","_c610","_c560","_c486","_c339","_c586","_c274","_c403","_c793","_c355","_c507","_c57","_c733","_c159","_c321","_c836","_c501","_c533","_c716","_c820","_c849","_c783","_c832","_c787","_c595","_c179","_c689","_c21","_c821","_c657","_c306","_c807","_c349","_c408","_c604","_c766","_c676","_c52","_c755","_c728","_c693","_c119","_c160","_c141","_c516","_c419","_c69","_c621","_c423","_c137","_c549","_c636","_c772","_c799","_c336","_c841","_c82","_c123","id","_c474","_c470","_c286","_c555","_c36","_c299","_c829","_c361","_c263","_c522","_c495","_c135")
val assembler = new VectorAssembler().setInputCols(columnNames).setOutputCol("features")
val output = assembler.transform(indexedDF) 
output.select("id", "label", "features").take(5).foreach(println)
val Array(trainingData, testData) = output.randomSplit(Array(0.9, 0.1), seed = 12345)

val copyTestData = testData.drop("id").drop("features")
copyTestData.coalesce(1).write.format("csv").option("header", "false").mode("overwrite").save("file:///Users/aurobindosarkar/Downloads/CNAE-9/input")

val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.3).setElasticNetParam(0.8)
// Fit the model
val lrModel = lr.fit(trainingData)
println("Model was fit using parameters: " + lrModel.parent.extractParamMap)
// Print the coefficients and intercept for multinomial logistic regression
println(s"Coefficients: \n${lrModel.coefficientMatrix}")
println(s"Intercepts: ${lrModel.interceptVector}")
val predictions = lrModel.transform(testData)
// Select example rows to display.
predictions.select("prediction", "label", "features").show(5)
// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

val lr = new LogisticRegression()
//val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.1, 0.01)).addGrid(lr.fitIntercept).addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)).build()
val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.1, 0.01)).addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)).build()
val cv = new CrossValidator().setEstimator(lr).setEvaluator(new MulticlassClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5) 
// Run cross-validation, and choose the best set of parameters.
val cvModel = cv.fit(trainingData)
// Make predictions on test documents. cvModel uses the best model found (lrModel).
cvModel.transform(testData).select("id", "label", "probability", "prediction").collect().foreach { case Row(id: Long, label: Double, prob: Vector, prediction: Double) =>
    println(s"($id, $label) --> prob=$prob, prediction=$prediction")
}
val cvPredictions = cvModel.transform(testData)
cvPredictions.select("prediction", "label", "features").show(5)
// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(cvPredictions)
println("Test Error = " + (1.0 - accuracy))

cvModel.write.overwrite.save("file:///Users/aurobindosarkar/Downloads/CNAE-9/model")

