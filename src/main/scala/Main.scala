import java.util.Date

import breeze.linalg.{DenseVector, norm}
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.elasticsearch.spark._

object Main {
  /**
    * Main entry point for the application.
 *
    * @param args The command line arguments.
    */
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

    //Parameters for the K Means model.
    val numClusters = 219 // Provides adequate accuracy.

    //Read training data from text file.
    val trainingData = sc.textFile(trainingDataPath).map(line => extractFeatures(line))

    //Creating a standardizer object that scales features.
    val scaler = new StandardScaler().fit(trainingData)

    //Scale training data.
    val scaledTrainingData = trainingData.map(d => scaler.transform(d)).cache

    //Train K Means model on existing data contained in regular file
    val kmeans = new KMeans().setK(numClusters).run(scaledTrainingData)
    val threshold = 0.2 //Threshold used to detect outliers.

    //Start stream to read from Kafka
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    //Process each message from Kafka, extract features and make prediction
    messages.flatMap(_._2.split("\n")).foreachRDD(rdd => {
      rdd.collect.foreach(message => {
        //Expected features: timeEpoch,label,byteLength,lastUpdate,travelDist,newLac,frequency
        val split = message.split(",")
        val timeEpoch = split(0).toDouble
        val label = split(1).toInt
        val byteLength = split(2).toDouble
        val lastUpdate = split(3).toDouble
        val travelDist = split(4).toDouble
        val newLac = split(5).toInt
        val frequency = split(6).toDouble

        val isInternal = isInternalMessage(newLac)
        val internalMsg = if (isInternal) 1 else 0
        val externalMsg = if (isInternal) 0 else 1

        val point = LabeledPoint(label, scaler.transform(Vectors.dense(byteLength, lastUpdate, travelDist, internalMsg, externalMsg, frequency)))

        val distScore = distToCentroid(point.features, kmeans) //Checking this points distance to the centroid

        val esMap = Map("timeEpoch" -> new Date(timeEpoch.toInt * 1000L), "label" -> point.label, "score" -> distScore)

        if (distScore > threshold) { //Possible anomaly.
          sc.makeRDD(Seq(esMap)).saveToEs("ss7-ml-results/anomaly")
        } else { //Possible normal traffic.
          sc.makeRDD(Seq(esMap)).saveToEs("ss7-ml-results/normal")
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Extracts features from a line containing the features.
 *
    * @param input The line containing the features read from a file.
    * @return A Vector containing the features.
    */
  def extractFeatures(input: String) = {
    val split = input.split(",")
    val byteLength = split(1).toDouble
    val lastUpdate = split(2).toDouble
    val travelDist = split(3).toDouble
    val newLac = split(4).toInt
    val frequency = split(5).toDouble

    val isInternal = isInternalMessage(newLac)

    val internalMsg = if(isInternal) 1 else 0
    val externalMsg = if(isInternal) 0 else 1

    Vectors.dense(byteLength, lastUpdate, travelDist, internalMsg, externalMsg, frequency)
  }

  /**
    * Checks if a message originates from the internal or external network.
 *
    * @param lac The LAC used to determine message origin.
    * @return True if the message originates from the internal network, false otherwise.
    */
  def isInternalMessage(lac: Int) = lac > 7000

  /**
    * Find the distance between the point and the centroid.
 *
    * @param datum The point.
    * @param model The k-means model containing the centroid.
    * @return Distance between point and centroid using the euclidean distance.
    */
  def distToCentroid(datum: Vector, model: KMeansModel) =
    distance(model.clusterCenters(model.predict(datum)), datum)

  /**
    * Distance between two points using the euclidean distance.
 *
    * @param V1 The first point.
    * @param V2 The second point.
    * @return The distance between the two points.
    */
  def distance(V1: Vector, V2: Vector) = {
    val a = DenseVector(V1.toArray)
    val b = DenseVector(V2.toArray)

    norm(a - b, 2)
  }
}
