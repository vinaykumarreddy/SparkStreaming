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
    logger.info("INFO")
    /*val surveyDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sample.csv")

    surveyDF.createOrReplaceTempView("survey_tbl")

    val countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

    logger.info(countDF.collect().mkString("->"))
    //scala.io.StdIn.readLine()
    spark.stop()*/
    // kafkastreams

    /*  val lineDf = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", "9092")
        .load()*/
    // tcp port by using net cat

    // ---->  Steps to perform for streaming  apps---->>   //

    // 1 Read data from streamSource - Input Data Frame
    // 2 Transform the data - >output DataFrame
    // 3 write output -sink

    // socket source   only for leaning
    //Rate Source only for testing
    // File source
    // Kafka source and file are commonly used in real time spark streaming projects


    val lineDf = spark.readStream
      .format("socket") //  here socket because the source is socket
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    // on lineDf we can apply Most of the all the transfomations which we apply on spark batch transformations


    // To verify the schema
    lineDf.printSchema()

    // Applying the Transformation on the input datasets
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
