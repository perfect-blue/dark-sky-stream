import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object Utillities {
  /**
   * konfigurasi logger sehingga hanya menampilkan pesan ERROR saja
   * untuk menghindari log spam
   */
  def setupLogging()={
    val logger = Logger.getRootLogger()
    logger.setLevel(Level.ERROR)
  }

  def writeQuery(query:DataFrame,mode:String,format:String): StreamingQuery ={
    val reesult=query.writeStream
      .outputMode(mode)
      .format(format)
      .start()

    reesult
  }

  def writeQueryKafka(query:DataFrame, mode:String,topic:String,hostPort:String,checkpoint:String):StreamingQuery={
    val ds=query.writeStream
      .format("kafka")
      .outputMode(mode)
      .option("kafka.bootstrap.servers",hostPort)
      .option("checkpointLocation", "/home/hduser/Desktop/checkpoint/monthly/"+checkpoint)
      .option("topic",topic)
      .start()

    ds
  }
}
