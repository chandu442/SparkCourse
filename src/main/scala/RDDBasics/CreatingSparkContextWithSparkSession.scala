package RDDBasics

import org.apache.spark.sql.SparkSession

object CreatingSparkContextWithSparkSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("creating spark context with spark session").master("local[*]").getOrCreate()
    val array = Array(1, 2, 3, 4, 5)

    // val sc = spark.sparkContext
    //val RDD=sc.parallelize(array,2)

    val RDD = spark.sparkContext.parallelize(array, 2)

    RDD.foreach(println)

    /*val sc = spark.sparkContext

    val file = "file:///media/orienit/BIGDATA/Videos/spark/talent origin/input/business-price-indexes-june-2019-quarter-csv.csv"
    val fileRDD = sc.textFile(file, 5)
    println("num of records in a file: " + fileRDD.count())

    println(fileRDD.first())*/
    val file = "file:///media/orienit/BIGDATA/Videos/spark/talent origin/input/business-price-indexes-june-2019-quarter-csv.csv"
    val fileRDD = spark.sparkContext.textFile(file)
    println("Num of records count in a file:  " + fileRDD.count())
    fileRDD.take(10).foreach(println)

  }
}
