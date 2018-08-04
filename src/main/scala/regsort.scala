import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._ 

object RegSort {  
    def main(args: Array[String]) {
    // val logFile = "./test.csv" // Should be some file on your system
        
    val spark = SparkSession
      .builder
      .appName("regsort")
      .config("es.index.auto.create", "true")
      .getOrCreate()

    val artistAndProducts = spark.sparkContext.esRDD("artists_and_products/_doc")

    // val logData = spark.read.textFile(logFile).cache()
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()

    val numAs = artistAndProducts.count()
    
    val numBs = 7777

    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}

