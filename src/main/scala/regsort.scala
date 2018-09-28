import java.time._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.elasticsearch.spark._
import org.apache.spark.sql._

import metrics.RegSlope

// import spark.implicits._

// For this program to run  in local mode on a macbook
// you need the folowing to your bash profile so that enough memory is allocated
// export _JAVA_OPTIONS="-Xms512m -Xmx4g"


object RegSort {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("regsort")
//      .config("es.index.auto.create", "true")
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

    //    val foodData = sqlContext.read.format("org.elasticsearch.spark.sql").load("artists_and_products/_doc")

    val foodData = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("./smallfood.csv")


    val slo = new RegSlope

    foodData
      .groupBy("Area", "Item")
      .agg(slo(col("year"), col("quantity")).as("slope"))
      .na
      .drop()
      .sort(desc("slope")).show(400)

    spark.stop()
  }
}
