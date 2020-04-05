import org.apache.spark.sql.types.{DataTypes, StructType}

object DarkSkySchema {
  //latitude,longitude,timezone
  val LATITUDE_FIELD = "latitude"
  val LONGITUDE_FIELD="longitude"
  val TIMEZONE_FIELD="timezone"

  //currently
  val CURRENTLY_FIELD="currently"
  val TIME_FIELD="time"
  val SUMMARY_FIELD="Summary"
  val PRECIP_INTENSITY_FIELD="precipIntensity"
  val PRECIP_PROBABILITY_FIELD="precipProbability"
  val PRECIP_TYPE_FIELD="precipType "
  val TEMPERATURE_FIELD="temperature"
  val APPARENT_TEMPERATURE_FIELD="apparentTemperature"
  val DEW_POINT_FIELD="dewPoint"
  val HUMIDITY_FIELD="humidity"
  val PRESSURE_FIELD="pressure"
  val WIND_SPEED_FIELD="windSpeed"
  val WIND_GUST_FIELD="windGust"
  val WIND_BEARING_FIELD="windBearing"
  val CLOUD_COVER_FIELD="cloudCover"
  val UV_INDEX_FIELD="uvIndex"
  val VISIBILITY_FIELD="visibility"
  val OZONE_FIELD="ozone"

  //date,data
  val DATE_FIELD="date"
  val DATA_FIELD="data"

  val CURRENTLY_STRUCT= new StructType()
    .add(CURRENTLY_FIELD,DataTypes.StringType)
    .add(TIME_FIELD,DataTypes.StringType)
    .add(SUMMARY_FIELD,DataTypes.StringType)
    .add(PRECIP_INTENSITY_FIELD,DataTypes.DoubleType)
    .add(PRECIP_PROBABILITY_FIELD,DataTypes.DoubleType)
    .add(PRECIP_TYPE_FIELD,DataTypes.DoubleType)
    .add(TEMPERATURE_FIELD,DataTypes.DoubleType)
    .add(APPARENT_TEMPERATURE_FIELD,DataTypes.DoubleType)
    .add(DEW_POINT_FIELD,DataTypes.DoubleType)
    .add(HUMIDITY_FIELD,DataTypes.DoubleType)
    .add(PRESSURE_FIELD,DataTypes.DoubleType)
    .add(WIND_SPEED_FIELD,DataTypes.DoubleType)
    .add(WIND_GUST_FIELD,DataTypes.DoubleType)
    .add(WIND_BEARING_FIELD,DataTypes.DoubleType)
    .add(CLOUD_COVER_FIELD,DataTypes.DoubleType)
    .add(UV_INDEX_FIELD,DataTypes.IntegerType)
    .add(VISIBILITY_FIELD,DataTypes.DoubleType)
    .add(OZONE_FIELD,DataTypes.DoubleType)

  val WEATHER_STRUCT=new StructType()
    .add(LATITUDE_FIELD,DataTypes.DoubleType)
    .add(LONGITUDE_FIELD,DataTypes.DoubleType)
    .add(TIMEZONE_FIELD,DataTypes.StringType)
    .add(CURRENTLY_FIELD,CURRENTLY_STRUCT)
    .add(DATE_FIELD,DataTypes.StringType)
    .add(DATA_FIELD,DataTypes.createArrayType(CURRENTLY_STRUCT))

  val PAYLOAD_STRUCT=new StructType()
    .add("payload",WEATHER_STRUCT)

}
