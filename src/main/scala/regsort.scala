import java.time._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.elasticsearch.spark._
import org.apache.spark.sql._

import metrics.RegSlope

// import spark.implicits._

// For this program to run sucessfully in local mode on a macbook
// add the folowing to your bash profiel
// export _JAVA_OPTIONS="-Xms512m -Xmx4g"


object RegSort {


  def main(args: Array[String]) {
    // val logFile = "./test.csv" // Should be some file on your system

    val spark = SparkSession
      .builder
      .appName("regsort")
      .config("es.index.auto.create", "true")
      // .config("spark.network.timeout", "600s")
      // .config("spark.executor.heartbeatInterval", "120s")
      // .config("spark.executor.memory", "2g")
      // .config("spark.driver.memory", "8g")
      // .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // val artistsAndProducts = spark.sparkContext.esRDD("artists_and_products/_doc")

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val pryphdf = sqlContext.read.format("org.elasticsearch.spark.sql").load("artists_and_products/_doc")
    // pryphdf.printSchema()

    // pryphdf.groupBy("artist").sum("real").withColumnRenamed("sum(real)", "dollaramount").sort(desc("dollaramount")).show()

    //    pryphdf.where('real.isNotNull).groupBy("artist").agg(sum("real"),max("real"),min("real")).sort(desc("sum(real)"))
    //    pryphdf.where('real.isNotNull).groupBy("artist").agg(sum("real").as("dollaramount"),max("real"),min("real")).sort(desc("dollaramount")).show

    val slo = new RegSlope

    pryphdf
      .where('real.isNotNull)
      .groupBy("artist")
      .agg(
        sum("real").as("dollaramount")
        ,max("real")
        ,slo(col("year"),col("month"),col("real")))
      .sort(desc("dollaramount")).show



    spark.stop()
  }
}




//
//  type ElasticDoc = (String, scala.collection.Map[String,AnyRef])
//
//  val onlyCreations = (doc : ElasticDoc ) => {
//    doc._2.contains("real")
//  }
//
//  val maxRealized = (doc1 : ElasticDoc, doc2 : ElasticDoc) => {
//    if (doc1._2("real").asInstanceOf[Long] > doc2._2("real").asInstanceOf[Long]){
//      doc1
//    }
//    else {
//      doc2
//    }
//  }
// val logData = spark.read.textFile(logFile).cache()
// val numAs = logData.filter(line => line.contains("a")).count()
// val numBs = logData.filter(line => line.contains("b")).count()

//    artistsAndProducts.filter(onlyCreations).take(2)
//    artistsAndProducts.filter(onlyCreations).first._2("real").getClass
//    artistsAndProducts.filter(onlyCreations).reduce(maxRealized)




//    val reader = spark.read
//      .format("org.elasticsearch.spark.sql")
//    // .option("es.nodes.wan.only","true")
// .option("es.port","443")
// .option("es.net.ssl","true")
// .option("es.nodes", esURL)

//    val df = reader.load("index/dogs").na.drop.orderBy($"breed")
//    display(df)
