package kafkaspark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._
object SendingKafka {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()
    while (true) {
      import spark.implicits._
      val apiUrl = "http://18.132.63.52:5000/file/api-data"
      val response = get(apiUrl, headers = headers)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      // select the columns you want to include in the message

      val messageDF = dfFromText.select($"age", $"city", $"dollar", $"firstname",
        $"id",$"lastname",$"state",$"street",$"zip")

      val kafkaServer: String = "ip-172-31-5-217.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "fraudDetection"

      messageDF.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("topic", topicSampleName).save()
      println("message is loaded to kafka topic")
      Thread.sleep(10000) // wait for 10 seconds before making the next call
    }
  }

}