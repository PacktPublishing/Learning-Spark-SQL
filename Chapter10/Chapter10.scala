//Code for Chapter 10 to be executed in Spark shell. For all other code from the BigDL library follow the instructions and commands in the book.
//Note that the code in this Chapter uses Spark 2.1 due to some bugs.

//Execute the following on the command prompt to start the Spark shell
source /Users/aurobindosarkar/Downloads/BigDL-master/scripts/bigdl.sh
Aurobindos-MacBook-Pro-2:spark-2.1.0-bin-hadoop2.7 aurobindosarkar$ bin/spark-shell --properties-file /Users/aurobindosarkar/Downloads/BigDL-master/spark/dist/target/bigdl-0.2.0-SNAPSHOT-spark-2.0.0-scala-2.11.8-mac-dist/conf/spark-bigdl.conf --jars /Users/aurobindosarkar/Downloads/BigDL-master/spark/dist/target/bigdl-0.2.0-SNAPSHOT-spark-2.0.0-scala-2.11.8-mac-dist/lib/bigdl-0.2.0-SNAPSHOT-jar-with-dependencies.jar

import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.dataset.DataSet
import com.intel.analytics.bigdl.dataset.image.{BytesToGreyImg, GreyImgNormalizer, GreyImgToBatch, GreyImgToSample}
import com.intel.analytics.bigdl.nn.{ClassNLLCriterion, Module}
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.utils.{Engine, LoggerFilter, T, Table}
import com.intel.analytics.bigdl.nn._
import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}
import com.intel.analytics.bigdl.dataset.ByteRecord
import com.intel.analytics.bigdl.utils.File

val trainData = "/Users/aurobindosarkar/Downloads/mnist/train-images-idx3-ubyte"
val trainLabel = "/Users/aurobindosarkar/Downloads/mnist/train-labels-idx1-ubyte"
val validationData = "/Users/aurobindosarkar/Downloads/mnist/t10k-images-idx3-ubyte"
val validationLabel = "/Users/aurobindosarkar/Downloads/mnist/t10k-labels-idx1-ubyte"
val nodeNumber = 1
val coreNumber = 2
Engine.init
val model = Sequential[Float]()
val classNum = 10
val batchSize = 12
model.add(Reshape(Array(1, 28, 28))).add(SpatialConvolution(1, 6, 5, 5)).add(Tanh()).add(SpatialMaxPooling(2, 2, 2, 2)).add(Tanh()).add(SpatialConvolution(6, 12, 5, 5)).add(SpatialMaxPooling(2, 2, 2, 2)).add(Reshape(Array(12 * 4 * 4))).add(Linear(12 * 4 * 4, 100)).add(Tanh()).add(Linear(100, classNum)).add(LogSoftMax())

def load(featureFile: String, labelFile: String): Array[ByteRecord] = {
    val featureBuffer = ByteBuffer.wrap(Files.readAllBytes(Paths.get(featureFile)))
    val labelBuffer = ByteBuffer.wrap(Files.readAllBytes(Paths.get(labelFile)));
    val labelMagicNumber = labelBuffer.getInt();
    require(labelMagicNumber == 2049);
    val featureMagicNumber = featureBuffer.getInt();
    require(featureMagicNumber == 2051);
    val labelCount = labelBuffer.getInt();
    val featureCount = featureBuffer.getInt();
    require(labelCount == featureCount);
    val rowNum = featureBuffer.getInt();
    val colNum = featureBuffer.getInt();
    val result = new Array[ByteRecord](featureCount);
    var i = 0;
    while (i < featureCount) {
      val img = new Array[Byte]((rowNum * colNum));
      var y = 0;
      while (y < rowNum) {
        var x = 0;
        while (x < colNum) {
          img(x + y * colNum) = featureBuffer.get();
          x += 1;
        }
        y += 1;
      }
      result(i) = ByteRecord(img, labelBuffer.get().toFloat + 1.0f);
      i += 1;
    }
    result;
  }
val trainMean = 0.13066047740239506
val trainStd = 0.3081078
val trainSet = DataSet.array(load(trainData, trainLabel), sc) -> BytesToGreyImg(28, 28) -> GreyImgNormalizer(trainMean, trainStd) -> GreyImgToBatch(batchSize)
val optimizer = Optimizer(model = model, dataset = trainSet, criterion = ClassNLLCriterion[Float]())   
val testMean = 0.13251460696903547
val testStd = 0.31048024
val maxEpoch = 2
val validationSet = DataSet.array(load(validationData, validationLabel), sc) -> BytesToGreyImg(28, 28) -> GreyImgNormalizer(testMean, testStd) -> GreyImgToBatch(batchSize)
optimizer.setEndWhen(Trigger.maxEpoch(2))
optimizer.setState(T("learningRate" -> 0.05, "learningRateDecay" -> 0.0))
optimizer.setCheckpoint("/Users/aurobindosarkar/Downloads/mnist/checkpoint", Trigger.severalIteration(500))
optimizer.setValidation(trigger = Trigger.everyEpoch, dataset = validationSet, vMethods = Array(new Top1Accuracy, new Top5Accuracy[Float], new Loss[Float]))
optimizer.optimize()
model.save("/Users/aurobindosarkar/Downloads/mnist/model")
val model = Module.load[Float]("/Users/aurobindosarkar/Downloads/mnist/model")
val partitionNum = 2
val rddData = sc.parallelize(load(validationData, validationLabel), partitionNum)
val transformer = BytesToGreyImg(28, 28) -> GreyImgNormalizer(testMean, testStd) -> GreyImgToSample()
val evaluationSet = transformer(rddData)
val result = model.evaluate(evaluationSet, Array(new Top1Accuracy[Float]), Some(batchSize))
result.foreach(r => println(s"${r._2} is ${r._1}"))

