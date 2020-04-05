import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Utillities._
import OutputSchema._
import DarkSkySchema._
import org.apache.spark.sql.streaming.StreamingQuery

class DarkSkyTask(sparkSession: SparkSession, bootstrapservers: String, topic:String) {
  import sparkSession.implicits._

  val train_load=sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",bootstrapservers)
    .option("subscribe",topic)
    .load()

  setupLogging()

  def subscribe():Unit={
    val string_df_train=train_load.selectExpr("CAST(Value AS STRING)")
    val structured_train=string_df_train.select(from_json($"Value",PAYLOAD_STRUCT) as("weather"))

    val weather_train=structured_train.selectExpr(
      "weather.payload.latitude",
      "weather.payload.longitude",
      "weather.payload.timezone",
      "weather.payload.currently.*")


    val weather_year=weather_train
      .withColumn("year",getYearFromTime(col("time")))
      .withColumn("month",getMonthFromTime(col("time")))
      .withColumn("day",getDayFromTime(col("time")))

    /*
     * monthly average temperature,dewPoint,Humidity,Pressure,Windspeed
     */
    val query_average_temperature=avgDataMonthly(weather_year,"temperature","complete",NEW_YORK+TEMPERATURE_MONTHLY)
    val query_average_apparent_temperature=avgDataMonthly(weather_year,"apparentTemperature","complete",NEW_YORK+APPARENT_TEMPERATURE_MONTHLY)
    val query_average_dew_point=avgDataMonthly(weather_year,"dewPoint","complete",NEW_YORK+DEW_POINT_MONTHLY)
    val query_average_humidity=avgDataMonthly(weather_year,"humidity","complete",NEW_YORK+HUMIDITY_MONTHLY)
    val query_average_pressure=avgDataMonthly(weather_year,"pressure","complete",NEW_YORK+PRESSURE_MONTHLY)
    val query_average_windspeed=avgDataMonthly(weather_year,"windspeed","complete",NEW_YORK+WIND_SPEED_MONTHLY)

    /*
     *yearly average temperature,dewPoint,Humidity,Pressure,Windspeed
     */

    query_average_temperature.awaitTermination()
    query_average_humidity.awaitTermination()
    query_average_pressure.awaitTermination()
    query_average_apparent_temperature.awaitTermination()
    query_average_windspeed.awaitTermination()
    query_average_dew_point.awaitTermination()

  }

  //analytics
  def avgDataMonthly(dataFrame:DataFrame,data:String,mode:String,output_topic:String):StreamingQuery={
    val avg_data=dataFrame
      .groupBy("year","month")
      .avg(data)

    val topic_df=avg_data.selectExpr("to_json(struct(year,month)) AS key","to_json(struct(*)) AS value")
    val query_data=writeQueryKafka(topic_df,mode,output_topic,this.bootstrapservers,data)
    query_data
  }
  //udf
  val getYearFromTime=sparkSession.udf.register("getYearFromTime",getYear)
  val getMonthFromTime=sparkSession.udf.register("getMonthFromTime",getMonth)
  val getDayFromTime=sparkSession.udf.register("getDayFromTime",getDay)

  def getYear=(time:String)=>{
    val timesplit=time.split("-")
    timesplit(0)
  }

  def getMonth=(time:String)=>{
    val timesplit=time.split("-")
    timesplit(1)
  }

  def getDay=(time:String)=>{
    val timesplit=time.split("-")
    timesplit(2)
  }
}
