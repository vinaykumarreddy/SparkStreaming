package guru.learningjournal.spark.examples

import guru.learningjournal.spark.examples.HelloSparkSQL.logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object KafkaStart {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hello Spark SQL")
      .master("local[3]")
      .config("spark-streaming.stopGracefullyOnshutdwn", "true")
      .config("spark-sql.shuffle.partitions", 1)
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.sql.streaming.checkpointLocation", "true")
      .getOrCreate()
    logger.info("INFO")

    val linedf = spark.readStream.format("json")
      .option("path", "input")
      .load()
    linedf.printSchema()

    // Transformation on datasets
    val frame = linedf.selectExpr("InvoiceNumber", "CreatedTime", "StoreId", "PosId", "CustomerType"
      , "PaymentMethod", "DeliveryType", "DeliveryAddress.City", "DeliveryAddress.State", "DeliveryAddress.PinCode",
      "explode(InvoiceLineItems) as LineItem")

    val flattendDf = frame
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("Itemprice", expr("LineItem.Itemprice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))

    val resu = flattendDf.writeStream
      .format("json")
      .option("path", "output")
      .option("checkpointlocation", "chkpointdir")
      .outputMode("append")
      .queryName("flattend-invoice")
      .start()


  }

}
