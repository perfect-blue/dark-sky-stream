import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import  Utillities._
import DarkSkySchema._

class DarkSkyTask(sparkSession: SparkSession, bootstrapservers: String, topic:String) {
  import sparkSession.implicits._

  val df_train=sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",bootstrapservers)
    .option("subscribe",topic)
    .load()

  setupLogging()

  def subscribe():Unit={
    val string_df_train=df_train.selectExpr("CAST(Value AS STRING)")
    val structured_train=string_df_train.select(from_json($"Value",PAYLOAD_STRUCT) as("weather"))

    val query=structured_train.selectExpr("weather.payload.latitude").writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
