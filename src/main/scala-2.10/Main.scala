import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.elasticsearch.spark._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.cloudera.spark.streaming.kafka.KafkaWriter._

/**
  * Created by kristoffer on 26.04.2016.
  */
object Main {
  def main(args: Array[String]): Unit = {
    val master = "spark://cruncher:7077"
    val appName = "SS7MLAnalysis"

    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("spark.cores.max", "8")

    // Elasticsearch configuration.
    conf.set("es.index.auto.create", "true")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    // Creating a K Means model.
    val numDimensions = 4
    val numClusters = 2
    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    //"Train" K Means model on existing data contained in regular file
    val trainingDataPath = "file:///home/kristoffer/spark-testing-data"
    val trainingData = ssc.textFileStream(trainingDataPath).map(line => {
      val split = line.split(",")
      val byteLength = split(1).toDouble
      val lastUpdate = split(2).toDouble
      val travelDist = split(3).toDouble
      val newLac = split(4).toDouble

      Vectors.dense(byteLength, lastUpdate, travelDist, newLac)
    })
    model.trainOn(trainingData)

    //Kafka input config
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "localhost:9092")
    val topics = Set("ss7-preprocessed")

    //Start stream to read from Kafka
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    //Process each message from Kafka and extract features
    val processedMessages = messages.flatMap(_._2.split("\n")).map(message => {
      //Expected features: timeEpoch,byteLength,lastUpdate,travelDist,newLac
      val split = message.split(",")
      val timeEpoch = split(0).toDouble
      val byteLength = split(1).toDouble
      val lastUpdate = split(2).toDouble
      val travelDist = split(3).toDouble
      val newLac = split(4).toDouble

      LabeledPoint(timeEpoch, Vectors.dense(byteLength, lastUpdate, travelDist, newLac))
    })

    //Make prediction on new input
    val predictions = model.predictOnValues(processedMessages.map(message => (message.label, message.features)))
    predictions.foreachRDD(rdd => {
      val rddCollect = rdd.collect()
      rddCollect.foreach(prediction => {
        //Save cluster assignment and label for sending to ES
        val esMap = Map[String,String](
          "label" -> prediction._1.toString,
          "cluster" -> prediction._2.toString
        )

        //Save cluster assignment in elasticsearch for visualization and preprocessing
        val esRDD = sc.makeRDD(Seq(esMap))
        esRDD.saveToEs("ss7-ml-results/clustering")
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
