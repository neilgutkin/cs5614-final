import java.util.Properties
import scala.io.Source
import java.nio.file.{Path, Paths}
import org.json4s._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.jackson.JsonMethods._

object Producer {
  def main(args: Array[String]): Unit = {
    implicit val formats: Formats = DefaultFormats
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val topic = "topic_test1"
    val currentDirectory: Path = Paths.get(System.getProperty("user.dir"))
    val dataDirectory: Path = Paths.get(currentDirectory.toString(), "/repo/dataset", "currentDatasetFinal.csv")
    val bufferedSource = Source.fromFile(dataDirectory.toString())
    val headerLine = bufferedSource.getLines.take(1).next
    for (line <- bufferedSource.getLines) {
      val dict = toDict(line)
      val myJValue = Extraction.decompose(dict)
      val value = compact(render(myJValue))
      println(value)
      val message = new ProducerRecord[String, String](topic, "dummykey", value)
      producer.send(message)
      Thread.sleep(1000) // adjust the rate at which the data is sent
    }
    bufferedSource.close
  }

  def toDict(line: String):  Map[String, Any] = {
    val arr = line.split(",")
    val station = arr(0)
    val valid = arr(1)
    val tmpf = arr(2)
    val dwpf = arr(3)
    val relh = arr(4)
    val feel = arr(5)
    val drct = arr(6)
    val sped = arr(7)
    val alti = arr(8)
    val mslp = arr(9)
    val p01m = arr(10)
    val vsby = arr(11)
    val skyc1 = arr(12)
    val skyl1 = arr(13)
    val wxcodes = arr(14)
    val ice_acceretion_1hr = arr(15)

    Map(
      "station" -> station,
      "valid" -> valid,
      "tmpf" -> tmpf,
      "dwpf" -> dwpf,
      "relh" -> relh,
      "feel" -> feel,
      "drct" -> drct,
      "sped" -> sped,
      "alti" -> alti,
      "mslp" -> mslp,
      "p01m" -> p01m,
      "vsby" -> vsby,
      "skyc1" -> skyc1,
      "skyl1" -> skyl1,
      "wxcodes" -> wxcodes,
      "ice_acceretion_1hr" -> ice_acceretion_1hr
    )
  }
}
