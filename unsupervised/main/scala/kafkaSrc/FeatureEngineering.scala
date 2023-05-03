package kafkaSrc

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import java.sql.Timestamp
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window



class FeatureEngineering(var kafkaStream: InputDStream[ConsumerRecord[String, String]], val spark: SparkSession) extends java.io.Serializable {
  import spark.sqlContext.implicits._
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

  def updateFunction(newValues: Seq[(Double, Timestamp, Timestamp)], runningCount: Option[List[(Double, Timestamp, Timestamp)]])
  : Option[List[(Double, Timestamp, Timestamp)]] = {
    val newList = runningCount.getOrElse(List[(Double, Timestamp, Timestamp)]()) ++ newValues
    Some(newList)
  }

  // feature Engineering and preprocessing starts here:
  def featureEngineering(df_formatted: DataFrame): DataFrame = {
    df_formatted.withColumn("mslp", when(col("mslp") === -1, 0).otherwise(col("mslp")))
      .withColumn("skyl1", when(col("skyl1") === 'M', 0).otherwise(col("skyl1")))
      .withColumn("wxcodes", when(col("wxcodes") === 'M', 88).otherwise(col("wxcodes")))
      .withColumn("ice_acceretion_1hr", when(col("ice_acceretion_1hr") === 'M', 0).otherwise(col("ice_acceretion_1hr")))
      .withColumn("skyc1", when(col("skyc1") === 'M', "CLR").otherwise(col("skyc1")))
      .withColumn("skyc1", when(col("skyc1") === "CLR", 0).otherwise(col("skyc1")))
      .withColumn("skyc1", when(col("skyc1") === "VV", 1).otherwise(col("skyc1")))
      .withColumn("skyc1", when(col("skyc1") === "SCT", 2).otherwise(col("skyc1")))
      .withColumn("skyc1", when(col("skyc1") === "FEW", 3).otherwise(col("skyc1")))
      .withColumn("skyc1", when(col("skyc1") === "OVC", 4).otherwise(col("skyc1")))
      .withColumn("skyc1", when(col("skyc1") === "BKN", 5).otherwise(col("skyc1")))
      .withColumn("station", when(col("station") === "CDA", 0).otherwise(col("station")))
      .withColumn("station", when(col("station") === "EFK", 1).otherwise(col("station")))
      .withColumn("station", when(col("station") === "FSO", 2).otherwise(col("station")))
      .withColumn("station", when(col("station") === "MVL", 3).otherwise(col("station")))
      .withColumn("station", when(col("station") === "RUT", 4).otherwise(col("station")))
      .withColumn("station", when(col("station") === "MPV", 5).otherwise(col("station")))
      .withColumn("station", when(col("station") === "VSF", 6).otherwise(col("station")))
      .withColumn("station", when(col("station") === "DDH", 7).otherwise(col("station")))
      .withColumn("station", when(col("station") === "1V4", 8).otherwise(col("station")))
      .withColumn("station", when(col("station") === "BTV", 9).otherwise(col("station")))
      .withColumn("station", when(col("station") === "6B0", 10).otherwise(col("station")))
  }


  def two_window(): DStream[((String, Timestamp), (Double, Timestamp, Timestamp))] = {
    val windowSize = 7200 // 2 hours in seconds
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

        val operatingDF = featureEngineering(df_formatted)
        operatingDF.rdd.map(row => {
          val validTimestamp = row.getAs[Timestamp]("valid")
          val window_start = validTimestamp.getTime / (windowSize * 1000)
          ((row.getAs[String]("station"), window_start),
            (row.getAs[Double]("tmpf"),
              new Timestamp(validTimestamp.getTime - (7 * 24 * 60 * 60 * 1000)),
              //                new Timestamp(validTimestamp.getTime)))
              new Timestamp(validTimestamp.getTime - (1 * 24 * 60 * 60 * 1000)
              )))
        })
      }
      .updateStateByKey(updateFunction)
      .transform { rdd =>
        rdd.flatMap { case ((station, window), temp_list) =>
          temp_list.map { case (temp, prevWeekStart, prevWeekEnd) =>
            ((station, window * windowSize * 1000), (temp, prevWeekStart, prevWeekEnd))
          }
        }
      }
      .map { case ((station, windowStart), (temp, prevWeekStart, prevWeekEnd)) =>
        ((station, new Timestamp(windowStart)), (temp, prevWeekStart, prevWeekEnd))
      }
  }

  def seven_window(): DStream[((String, Timestamp, Timestamp), (Double, Double))] = {
    kafkaStream.map(record => record.value)
      .transform(rdd => {
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
          .withColumn("tmpf", $"tmpf".cast("double")) //

        val operatingDF = featureEngineering(df_formatted)

        val windowSize = 7 * 24 * 60 * 60 // 7 days in seconds
        val slideInterval = 24 * 60 * 60 // 1 day in seconds

        val windowSpec = Window.partitionBy("station")
          .orderBy($"valid".cast("timestamp").cast("long"))
          .rangeBetween(-windowSize, 0)

        val df_windowed = operatingDF
          .withColumn("temps", collect_list($"tmpf").over(windowSpec))
          .withColumn("windowStart",
            when(
              $"valid" >= to_timestamp(lit("2013-01-01 00:00:00")) && $"valid" < to_timestamp(lit("2013-01-08 00:00:00")),
              to_timestamp(lit("2013-01-01 00:00:00"))
            ).otherwise($"valid" - expr(s"INTERVAL $windowSize seconds")))

          .groupBy("station", "windowStart", "valid", "temps")
          .agg(
            expr("AGGREGATE(temps, (0.0D, 0.0D, 0L), (acc, x) -> (acc.col1 + x, acc.col2 + x * x, acc.col3 + 1), acc -> (acc.col1 / acc.col3, SQRT(acc.col2 / acc.col3 - (acc.col1 / acc.col3) * (acc.col1 / acc.col3))))").alias("avgStdTemp")
          )
          .rdd
          .map(row => (
            (row.getAs[String]("station"), row.getAs[Timestamp]("windowStart"), row.getAs[Timestamp]("valid")),
            (row.getAs[Row]("avgStdTemp").getDouble(0), row.getAs[Row]("avgStdTemp").getDouble(1))
          ))
        df_windowed
      })
  }


}
