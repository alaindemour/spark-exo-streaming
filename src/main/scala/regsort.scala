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
      // .config("spark.network.timeout", "600s")
      // .config("spark.executor.heartbeatInterval", "120s")
      // .config("spark.executor.memory", "2g")
      // .config("spark.driver.memory", "8g")
      // .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()

    import spark.implicits._


    spark.sparkContext.setLogLevel("ERROR")

//    val sqlContext = spark.sqlContext
//    import sqlContext.implicits._

    val foodData = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("./small-food.csv")


    val dataSchema = foodData.schema

    val foodDataStreaming = spark
      .readStream
      .schema(dataSchema)
      .option("header", "true")
      .option("maxFilesPerTirgger",1)
      .csv("./*.csv")

    val slo = new RegSlope

    val foodQuery = foodDataStreaming
      .groupBy("Area", "Item")
      .agg(slo(col("year"), col("quantity")).as("slope"))
      .na
      .drop()
// you cannot sort a stream aggregation
//      .sort(desc("slope")).show(400)

    val activityQuery = foodQuery
      .writeStream
      .queryName("food_result_table")
      .format("memory")
      .outputMode("complete")
      .start()



    for (i <- 1 to 10){
      spark.sql("SELECT * FROM food_result_table").show()
      Thread.sleep(1000)
    }


    activityQuery.awaitTermination()


  }
}
