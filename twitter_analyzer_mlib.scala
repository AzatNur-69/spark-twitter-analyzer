

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object TestScala {

  /*Init Spark Context*/
  def initSparkContext(jars_directory: String): Unit = {
    /*  Initialize Spark Context*/
    val conf: SparkConf = new SparkConf()
      .setAppName("gdelt")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "2g")
      .set("spark.driver.maxResultSize", "2g")
      //.setMaster("spark://10.8.41.146:7077")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    // Configure Twitter credentials

    val apiKey = "0"
    val apiSecret = "0"
    val accessToken = "0"
    val accessTokenSecret = "0"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    /*  Initialize SparkSQL Context*/
    val sqlContext = new SQLContext(sc)
    val gson = new GsonBuilder().setPrettyPrinting().create()
    val jsonParser = new JsonParser()
    val tweetInput = "C:\\SparkData\\tweets\\tweets_[0-9]*\\part-[0-9]*"

    /*examine tweets*/
    val tweets = sc.textFile(tweetInput)

    for (tweet <- tweets.take(5)) {
      println(gson.toJson(jsonParser.parse(tweet)))
    }

    val tweetTable = sqlContext.jsonFile(tweetInput)
    tweetTable.registerTempTable("tweetTable")
    tweetTable.printSchema()

    sqlContext.sql(
      "SELECT text FROM tweetTable LIMIT 10")
      .collect().foreach(println)

    sqlContext.sql(
      "SELECT user.lang, user.name, text FROM tweetTable LIMIT 10")
      .collect().foreach(println)

    sqlContext.sql(
      "SELECT user.lang, COUNT(*) as cnt FROM tweetTable " +
        "GROUP BY user.lang ORDER BY cnt DESC limit 1000")
      .collect.foreach(println)

    val texts = sqlContext
      .sql("SELECT text from tweetTable")
      .rdd
      .map(row => row.get(0).toString)

    // Cache the vectors RDD since it will be used for all the KMeans iterations.

    val vectors = texts
      .map(Utils.featurize)
      .cache()

    val numClusters = 10

    val numIterations = 40

    vectors.count() // Calls an action on the RDD to populate the vectors cache.

    // Train KMenas model and save it to file

    val model = KMeans.train(vectors, numClusters, numIterations)

    val modelOutput = "C:\\SparkData\\Kmeans_model"
    model.save(sc, modelOutput)

    println("----100 example tweets from each cluster")

    0 until numClusters foreach { i =>

      println(s"\nCLUSTER $i:")

      texts.take(100) foreach { t =>

        if (model.predict(Utils.featurize(t)) == i) println(t)

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


}



