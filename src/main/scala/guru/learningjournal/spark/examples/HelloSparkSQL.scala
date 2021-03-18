package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{SparkSession, functions}

object HelloSparkSQL extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hello Spark SQL")
      .master("local[3]")
      .config("spark-streaming.stopGracefullyOnshutdwn", "true")
      .config("spark-sql.shuffle.partitions", 1)
      .getOrCreate()

    /*val surveyDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sample.csv")

    surveyDF.createOrReplaceTempView("survey_tbl")

    val countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

    logger.info(countDF.collect().mkString("->"))
    //scala.io.StdIn.readLine()
    spark.stop()*/
    val lineDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9092")
      .load()
    lineDf.printSchema()
    val wordDf = lineDf.select(expr("explode(split(value,' ')) as word"))
    val countDf = wordDf.groupBy("word").count()
    val query = countDf.writeStream.format("console")
      .option("checkpointlocation", "check-point-dir").outputMode("complete")
      .start()
    logger.info("INFO")
    logger.info("listenijng 9999")
    query.awaitTermination()
  }
}
