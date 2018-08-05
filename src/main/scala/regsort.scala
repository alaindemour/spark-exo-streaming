import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._ 
// import spark.implicits._




object RegSort {  

    val onlyCreations = (doc : (String, scala.collection.Map[String,AnyRef])) => { 
      doc._2.contains("real")
    }  

    def main(args: Array[String]) {
    // val logFile = "./test.csv" // Should be some file on your system
        
    val spark = SparkSession
      .builder
      .appName("regsort")
      .config("es.index.auto.create", "true")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "120s")
      .config("spark.executor.memory", "2g")
      .config("spark.driver.memory", "8g")
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val artistsAndProducts = spark.sparkContext.esRDD("artists_and_products/_doc")

    // val logData = spark.read.textFile(logFile).cache()
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    
    artistsAndProducts.filter(onlyCreations).collect()

    spark.stop()
  }
}

