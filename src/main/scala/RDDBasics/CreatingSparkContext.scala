package RDDBasics

import org.apache.spark.{SparkConf, SparkContext}

object CreatingSparkContext {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("First Spark Application")
    sparkConf.setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)

    val RDD = sc.parallelize(array, 2)

    println("num of elements in RDD:  " + RDD.count())

    RDD.foreach(println)
    val file = "file:///media/orienit/BIGDATA/Videos/spark/talent origin/input/business-price-indexes-june-2019-quarter-csv.csv"
    val fileRDD = sc.textFile(file, 5)
    println("num of records in a file: " + fileRDD.count())

    println(fileRDD.first())


  }

}
