import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object ReadingKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaToJson").master("local[*]").getOrCreate()

    // Define the Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-5-217.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Define the Kafka topic to subscribe to
    val topic = "fraudDetection"

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("age", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("dollar", StringType, nullable = true),
      StructField("firstname", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("lastname", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("street", StringType, nullable = true),
      StructField("zip", StringType, nullable = true)

    ))

    // Read the JSON messages from Kafka as a DataFrame
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-5-217.eu-west-2.compute.internal:9092").option("subscribe", topic).option("startingOffsets", "earliest").load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")

    // Write the DataFrame as CSV files to HDFS
    df.writeStream.format("csv").option("checkpointLocation", "/tmp/jenkins/kafka/trainarrival/checkpoint").option("path", "/tmp/jenkins/kafka/trainarrival/data").start().awaitTermination()

  }

}