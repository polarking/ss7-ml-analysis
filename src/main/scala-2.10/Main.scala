import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.Normalizer
import org.elasticsearch.spark._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object Main {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val appName = "SS7MLAnalysis"

    val user = args(1)
    val pass = args(2)
    val trainingDataPath = args(3) //Directory of the form: file:///home/kristoffer/spark-testing-data

    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("spark.cores.max", "8")

    // Elasticsearch configuration.
    conf.set("es.resource", "ss7-ml-results/results")
    conf.set("es.index.auto.create", "true")
    conf.set("es.net.http.auth.user", user)
    conf.set("es.net.http.auth.pass", pass)

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    // Creating a K Means model.
    val numDimensions = 3
    val numClusters = 2
    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    //Creating a normalizer object that normalizes features with L^2 norm.
    val normalizer = new Normalizer()

    //"Train" K Means model on existing data contained in regular file
    val trainingData = ssc.textFileStream(trainingDataPath).map(line => {
      val split = line.split(",")
      val byteLength = split(1).toDouble
      val lastUpdate = split(2).toDouble
      val travelDist = split(3).toDouble

      normalizer.transform(Vectors.dense(byteLength, lastUpdate, travelDist))
    })
    model.trainOn(trainingData)

    //Kafka input config
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "localhost:9092")
    val topics = Set("ss7-preprocessed")

    //Start stream to read from Kafka
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    //Process each message from Kafka and extract features
    val processedMessages = messages.flatMap(_._2.split("\n")).map(message => {
      //Expected features: label,byteLength,lastUpdate,travelDist
      val split = message.split(",")
      val label = split(0).toInt
      val byteLength = split(1).toDouble
      val lastUpdate = split(2).toDouble
      val travelDist = split(3).toDouble

      LabeledPoint(label, normalizer.transform(Vectors.dense(byteLength, lastUpdate, travelDist)))
    })

    //Make prediction on new input
    val predictions = model.predictOnValues(processedMessages.map(message => (message.label, message.features)))
    predictions.foreachRDD(rdd => {
      val rddCollect = rdd.collect()
      rddCollect.foreach(prediction => {
        //Save cluster assignment and label for sending to ES
        val esMap = Map("label" -> prediction._1, "cluster" -> prediction._2)

        //Save cluster assignment in elasticsearch for visualization and preprocessing
        val esRDD = sc.makeRDD(Seq(esMap))
        esRDD.saveToEs("ss7-ml-results/results")
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
