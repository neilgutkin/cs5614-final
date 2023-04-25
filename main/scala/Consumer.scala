import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// import org.apache.spark.rdd.MapPartitionsRDDclass
// import org.apache.spark.sql.Datasetclass
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.{avg, col, window, stddev}
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.sql.Timestamp

import scala.reflect.ClassTag
import org.apache.spark.sql.functions.udf


object Consumer {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName("Spark Kafka Example")
      .master("local[*]")
      .config("spark.sql.codegen.hugeMethodLimit", "32000")
      .config("spark.sql.codegen.wholeStage", false)
      .getOrCreate()
    import spark.implicits._

    // Your Kafka code goes here
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-kafka-example",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topic_test1")
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))
    streamingContext.checkpoint("checkpoint")
    // this is your dstream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val schema = StructType(Array(
      StructField("station", StringType, true),
      StructField("valid", StringType, true),
      StructField("tmpf", StringType, true),
      StructField("dwpf", StringType, true),
      StructField("relh", StringType, true),
      StructField("feel", StringType, true),
      StructField("drct", StringType, true),
      StructField("sped", StringType, true),
      StructField("alti", StringType, true),
      StructField("mslp", StringType, true),
      StructField("p01m", StringType, true),
      StructField("vsby", StringType, true),
      StructField("skyc1", StringType, true),
      StructField("skyl1", StringType, true),
      StructField("wxcodes", StringType, true),
      StructField("ice_acceretion_1hr", StringType, true)
    ))

    def updateFunction(newValues: Seq[Double], runningCount: Option[List[Double]]): Option[List[Double]] = {
      val newList = runningCount.getOrElse(List[Double]()) ++ newValues
      Some(newList)
    }

    def two_window()={
      kafkaStream.map(record => record.value)
        .transform { rdd =>
          val df_parsed = rdd.toDF("value")
            .select(from_json($"value", schema).as("data"))
            .select(
              date_format($"data.valid", "yyyy-MM-dd HH:mm:ss").as("valid"), // <-- example format
              $"data.station",
              $"data.tmpf",
              $"data.dwpf",
              $"data.relh",
              $"data.feel",
              $"data.drct",
              $"data.sped",
              $"data.alti",
              $"data.mslp",
              $"data.p01m",
              $"data.vsby",
              $"data.skyc1",
              $"data.skyl1",
              $"data.wxcodes",
              $"data.ice_acceretion_1hr")

          val df_formatted = df_parsed
            .withColumn("valid", to_timestamp($"valid", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("tmpf", $"tmpf".cast("double"))

          df_formatted.rdd.map(row => ((row.getAs[String]("station"), row.getAs[Timestamp]("valid").getHours / 2), row.getAs[Double]("tmpf")))
        }
        .updateStateByKey(updateFunction)
        .foreachRDD { rdd =>
          rdd.map { case ((station, window), temp_list) =>
            (station, window * 2, (window + 1) * 2, temp_list)
          }.toDF("station", "window_start", "window_end", "temp_list")
            .show()
        }
    }

    def seven_window(): Any = {
      def average: (Seq[Double] => Double) = seq => seq.sum / seq.size


      val averageUDF = udf(average)

      def standardDeviation: (Seq[Double] => Double) = { values =>
        val mean = values.sum / values.length
        val squaredDiffs = values.map(value => (value - mean) * (value - mean))
        math.sqrt(squaredDiffs.sum / squaredDiffs.length)
      }
      var model = new StreamingKMeans()
        .setDecayFactor(0.3)
        .setK(4)
        .setRandomCenters(5, 100.0)
      val stddevUDF = udf(standardDeviation)
      kafkaStream.map(record => println(record))
      kafkaStream.map(record => record.value)
        .foreachRDD(rdd => {
          val df_parsed = rdd.toDF("value")
            .select(from_json($"value", schema).as("data"))
            .select(
              date_format($"data.valid", "yyyy-MM-dd HH:mm:ss").as("valid"),
              $"data.station",
              $"data.tmpf",
              $"data.dwpf",
              $"data.relh",
              $"data.feel",
              $"data.drct",
              $"data.sped",
              $"data.alti",
              $"data.mslp",
              $"data.p01m",
              $"data.vsby",
              $"data.skyc1",
              $"data.skyl1",
              $"data.wxcodes",
              $"data.ice_acceretion_1hr")

          val df_formatted = df_parsed
            .withColumn("valid", to_timestamp($"valid", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("tmpf", $"tmpf".cast("double"))

          val df_stream = df_formatted
            .groupBy($"station", window($"valid", "7 days", "15 minutes"))
            .agg(collect_list("tmpf").alias("temp_list"))
            .select(
              $"station",
              $"window.start".as("window_start"),
              $"window.end".as("window_end"),
              $"temp_list"
            )

          // df_stream.show(truncate=false)


          val df_results = df_stream
            .withColumn("avg_temp", averageUDF($"temp_list"))
            .withColumn("stddev_temp", stddevUDF($"temp_list"))
          val rddf = df_results.rdd
          rddf.collect().foreach(println)
//          var model = new StreamingKMeans()
//            .setDecayFactor(0.3)
//            .setK(4)
//            .setRandomCenters(5, 100.0)
//            .trainOn(df_results)
//          model.trainOn(Vector)
//          model.predictOn(Vector)
          // var model: StreamingKMeans = model.trainOn(df_results)
//          df_results.show()
        })
    }
    seven_window()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
