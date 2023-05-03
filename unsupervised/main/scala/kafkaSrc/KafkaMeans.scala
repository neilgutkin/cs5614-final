package kafkaSrc
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.{StreamingKMeans, StreamingKMeansModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import java.io.{File, FileOutputStream, InputStream, IOException, PrintWriter}
import java.io.Serializable
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

class KafkaMeans(var decayFactor: Double = 5, var k: Int = 3, var thold: Int = 1) extends Serializable{

  val kafkaModel: StreamingKMeans = new StreamingKMeans()
    .setDecayFactor(this.decayFactor)
    .setK(this.k)
    .setRandomCenters(this.k, 100.0)

  private def _distanceFromCentroid(input: Vector, model: StreamingKMeansModel, prediction: Int) = {
    val centroid: Vector = model.clusterCenters(prediction)
    Math.sqrt(input.toArray.zip(centroid.toArray).map(tup => Math.pow(tup._1 - tup._2, 2)).sum)
  }

  def train(dstream: DStream[Vector]): Unit = {
    kafkaModel.trainOn(dstream)
  }


  def evaluate(dstream: DStream[Vector], spark: SparkSession): RDD[Boolean] = {
    val modelBcast = spark.sparkContext.broadcast(kafkaModel.latestModel())
    val model: StreamingKMeansModel = modelBcast.value
    var anomalies: RDD[Boolean] = spark.sparkContext.emptyRDD[Boolean]
    val pw = new PrintWriter(new FileOutputStream(new File("log_backup.txt"), true))
    pw.append("Normalized Humidity, Normalized Visibility, Z-Score, Prediction\n")
    pw.close()

    dstream.foreachRDD { rddVec: RDD[Vector] =>
      val pw = new PrintWriter(new FileOutputStream(new File("log_backup.txt"), true))

      val distRDD: RDD[Double] = rddVec.map { vec: Vector =>
        val prediction: Int = model.predict(vec)
        _distanceFromCentroid(input = vec, model = model, prediction = prediction)
      }

      val n: Long = distRDD.count()
      val avg: Double = distRDD.sum() / n
      val stdDev: Double = {
        Math.sqrt(distRDD.map(x => Math.pow(x - avg, 2)).sum / (n - 1))
      }

      // transform the vector
      val normalizedDists: RDD[Double] = distRDD.map(dist => (dist - avg) / stdDev)

      val currentAnomalies: RDD[Boolean] = normalizedDists.map(dist => dist > thold)
      anomalies = anomalies.union(currentAnomalies)

      // output
      val outputRdd: RDD[(Vector, Boolean)] = rddVec.zip(currentAnomalies)
      pw.append(outputRdd.collect().mkString("\n"))
      pw.close()
    }
    train(dstream = dstream)
    anomalies
  }
}