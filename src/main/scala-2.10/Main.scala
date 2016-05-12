import breeze.linalg.{norm, DenseVector}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.{StandardScaler, Normalizer}
import org.elasticsearch.spark._
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans, StreamingKMeansModel, StreamingKMeans}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object Main {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val appName = "SS7MLAnalysis"

    val user = args(1)
    val pass = args(2)
    val trainingDataPath = args(3) //File path of the form: file:///home/kristoffer/spark-testing-data

    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)

    // Elasticsearch configuration.
    conf.set("es.resource", "ss7-ml-results/results")
    conf.set("es.index.auto.create", "true")
    conf.set("es.net.http.auth.user", user)
    conf.set("es.net.http.auth.pass", pass)

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    //Kafka input config
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "localhost:9092")
    val topics = Set("ss7-preprocessed")

    // Creating a K Means model.
    val numDimensions = 4
    val numClusters = 219 // Provides adequate accuracy.

    //Read training data from textfile.
    val trainingData = sc.textFile(trainingDataPath).map(line => extractFeatures(line))

    //Creating a standardizer object that standardizes features.
    val scaler = new StandardScaler().fit(trainingData)

    //Scale training data.
    val scaledTrainingData = trainingData.map(d => scaler.transform(d))

    //Train K Means model on existing data contained in regular file
    val kmeans = new KMeans().setK(numClusters).run(scaledTrainingData)
    val threshold = 1

    //Start stream to read from Kafka
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    //Process each message from Kafka, extract features and make prediction
    messages.flatMap(_._2.split("\n")).foreachRDD(rdd => {
      rdd.collect.foreach(message => {
        //Expected features: label,byteLength,lastUpdate,travelDist,newLac
        val split = message.split(",")
        val label = split(0).toInt
        val byteLength = split(1).toDouble
        val lastUpdate = split(2).toDouble
        val travelDist = split(3).toDouble
        val newLac = split(4).toInt

        val isInternal = isInternalMessage(newLac)
        val internalMsg = if (isInternal) 1 else 0
        val externalMsg = if (isInternal) 0 else 1

        val point = LabeledPoint(label, scaler.transform(Vectors.dense(byteLength, lastUpdate, travelDist, internalMsg, externalMsg)))

        val distScore = distToCentroid(point.features, kmeans) //Checking this points distance to the centroid

        if (distScore > threshold) { //Possible anomaly.
          val esMap = Map("label" -> point.label, "score" -> distScore)
          sc.makeRDD(Seq(esMap)).saveToEs("ss7-ml-results/anomaly")
        } else { //Possible normal traffic.
          val esMap = Map("label" -> point.label, "score" -> distScore)
          sc.makeRDD(Seq(esMap)).saveToEs("ss7-ml-results/normal")
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def extractFeatures(input: String) = {
    val split = input.split(",")
    val byteLength = split(1).toDouble
    val lastUpdate = split(2).toDouble
    val travelDist = split(3).toDouble
    val newLac = split(4).toInt

    val isInternal = isInternalMessage(newLac)

    val internalMsg = if(isInternal) 1 else 0
    val externalMsg = if(isInternal) 0 else 1

    Vectors.dense(byteLength, lastUpdate, travelDist, internalMsg, externalMsg)
  }

  def isInternalMessage(lac: Int) = lac > 7000

  def distToCentroid(datum: Vector, model: KMeansModel) =
    distance(model.clusterCenters(model.predict(datum)), datum)

  def distance(centroid: Vector, datum: Vector) = {
    val a = DenseVector(centroid.toArray)
    val b = DenseVector(datum.toArray)

    norm(a - b, 2)
  }
}
