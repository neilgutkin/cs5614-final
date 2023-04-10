import java.util.Properties
import java.io.File
import scala.io.Source
import scala.util.Random
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {
  def main(args: Array[String]): Unit = {
    implicit val formats: Formats = DefaultFormats
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val topic = "topic_test"
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val bufferedSource = Source.fromFile(currentDirectory+"/dataset/datasetEncodedV3.csv")
    val headerLine = bufferedSource.getLines.take(1).next
    for (line <- bufferedSource.getLines) {
      // println(line)
      val dict = toDict(line)
      val myJValue = Extraction.decompose(dict)
      val value = compact(render(myJValue))
      // println(value)
      println(value)
      val message = new ProducerRecord[String, String](topic, "dummykey", value)
      producer.send(message)
      Thread.sleep(5000) // adjust the rate at which the data is sent
    }
    bufferedSource.close
  }

  def toDict(line: String):  Map[String, Any] = {
    val arr = line.split(",")
    val valid = arr(0)
    val tmpf = arr(1)
    val dwpf = arr(2)
    val relh = arr(3)
    val feel = arr(4)
    val drct = arr(5)
    val sped = arr(6)
    val alti = arr(7)
    val p01m = arr(8)
    val vsby = arr(9)
    val skyc1 = arr(10)
    val skyl1 = arr(11)
    val wxcodes = arr(12)
    val station_encoded = arr(13)
    val skyc1_encoded = arr(14)

    Map(
      "valid" -> valid,
      "tmpf" -> tmpf,
      "dwpf" -> dwpf,
      "relh" -> relh,
      "feel" -> feel,
      "drct" -> drct,
      "sped" -> sped,
      "alti" -> alti,
      "p01m" -> p01m,
      "vsby" -> vsby,
      "skyc1" -> skyc1,
      "skyl1" -> skyl1,
      "wxcodes" -> wxcodes,
      "station_encoded" -> station_encoded,
      "skyc1_encoded" -> skyc1_encoded,
    )
  }
}
