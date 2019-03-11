import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestScala {
  /*Init Spark Context*/
  def initSparkContext(jars_directory:String):Unit =
  {
    /*  Initialize Spark Context*/
    val conf: SparkConf = new SparkConf()
      .setAppName("gdelt")
      .set("spark.executor.memory", "18g")
      .set("spark.driver.memory", "18g")
      .set("spark.driver.maxResultSize","18g")
      //.setMaster("spark://10.8.41.146:7077")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.addJar(jars_directory+"/commons-csv-1.1.jar")
    sc.addJar(jars_directory+"/spark-csv_2.10-1.4.0.jar")
    sc.addJar(jars_directory+"/univocity-parsers-1.5.1.jar")

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.setAppName("spark-sreaming")

    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    // Configure Twitter credentials

    val apiKey = "4WSlbmjZYJQenHNTkEIPSsn4q"
    val apiSecret = "kuAbPWyqyGDsFlZEM6f9vNGHyp8OVcr3g0P8jOZUsqOdMfl3AB"

    val accessToken = "134343021-sfj30mj6KAKHvJ5H8zPkbOO53HHTt5f5xm2oSlGJ"

    val accessTokenSecret = "VSxBjZ9SFjFAwBsvFxAsTAL9amIORBeXGUnDgW27bEbUg"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)

    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)

    System.setProperty("twitter4j.oauth.accessToken", accessToken)

    System.setProperty("twitter4j.oauth.accessTokenSecret",

      accessTokenSecret)


/*model in real time*/
    println("Initializing Twitter stream...")

    val tweets = TwitterUtils.createStream(ssc, None)

    val statuses = tweets.map(_.getText)

    val modelFile = "C:\\SparkData\\Kmeans_model"

    println("Initializing the KMeans model...")

    val model = KMeansModel.load(sc, modelFile)

    val clusterNumber = 5 //arabic language

    val filteredTweets = statuses

      .filter(t => model.predict(Utils.featurize(t)) == clusterNumber)

    filteredTweets.print()





    ssc.start()

    ssc.awaitTermination()

  }

}




