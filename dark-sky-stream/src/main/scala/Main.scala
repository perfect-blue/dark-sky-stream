import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val bootstrapServers="127.0.0.1:9092"
    val session=initializeSpark("weather-streaming","local[*]")
    val darkSkyTask:DarkSkyTask=new DarkSkyTask(session,bootstrapServers,"weather.train")
    darkSkyTask.subscribe()
  }

  def initializeSpark(name:String,master:String): SparkSession={
    val session=SparkSession
      .builder()
      .appName(name)
      .master(master)
      .getOrCreate()

    return session
  }
}
