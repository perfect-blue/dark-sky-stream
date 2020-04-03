import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import  Utillities._
import DarkSkySchema._

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


    val weather_year=weather_train.withColumn("year",getYearFromTime(col("time")))

    //analytic
    //temperature
    val average_temperature=weather_year
      .groupBy("year")
      .avg("temperature")
    val query_average_temperature=writeQuery(average_temperature,"complete","console")

    //apparent temperature
    val average_apparent_temperature=weather_year
      .groupBy("year")
      .avg("apparentTemperature")
    val query_average_apparent_temperature=writeQuery(average_apparent_temperature,"complete","console")

    //dew point
    val dew_point_average=weather_year
      .groupBy("year")
      .avg("dewPoint")
    val dew_point_query=writeQuery(dew_point_average,"complete","console")

    //humidity
    val average_humidity=weather_year
      .groupBy("year")
      .avg("humidity")
    val query_average_humidity=writeQuery(average_humidity,"complete","console")

    //pressure
    val pressure_average=weather_year
      .groupBy("year")
      .avg("pressure")
    val query_average_pressure=writeQuery(pressure_average,"complete","console")

    //windspeed
    val average_wind_speed=weather_year
        .groupBy("year")
        .avg("humidity")
    val query_average_windspeed=writeQuery(average_wind_speed,"complete","console")

    query_average_temperature.awaitTermination()
    query_average_humidity.awaitTermination()
    query_average_pressure.awaitTermination()
    query_average_apparent_temperature.awaitTermination()
    query_average_windspeed.awaitTermination()
  }

  //udf
  val getYearFromTime=sparkSession.udf.register("getYearFromTime",getYear)
  def getYear=(time:String)=>{
    val timesplit=time.split("-")
    timesplit(0)
  }

}
