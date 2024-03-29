package test

import javax.xml.bind.annotation._
import org.apache.beam.sdk.io.xml.XmlIO
import com.spotify.scio._
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter
import org.apache.beam.sdk.io.{Compression, FileIO}

import scala.annotation.meta._
import test.XmlTypes.{xmlElement, _}

object WordCount extends Xml {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val exampleData = "gs://dataflow-samples/shakespeare/kinglear.txt"
    val input = args.getOrElse("input", exampleData)

    val scol = sc.textFile(input)
      .map(_.trim)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .take(10)
      .map(x => Dupa(x))
      .saveAsCustomOutput("output", flow)

    val result = sc.close().waitUntilFinish()
  }
}

@xmlRootElement(name = "dupa")
case class Dupa(@xmlValue foo: String) {
  private def this() = this("")
}


trait Xml {
  val xmlioSink: XmlIO.Sink[Dupa] = XmlIO
    .sink(classOf[Dupa])
    .withRootElement("words")

  val flow: FileIO.Write[Void, Dupa] =  FileIO
    .write[Dupa]()
    .withCompression(Compression.GZIP)
    .withNumShards(1)
    .via(xmlioSink)
    .to("output")

}

object XmlTypes {
  type xmlAccessorType = XmlAccessorType @companionClass
  type xmlAttribute = XmlAttribute @field
  type xmlElement = XmlElement @field
  type xmlRootElement = XmlRootElement @companionClass
  type xmlTypeAdapter = XmlJavaTypeAdapter @field
  type xmlValue = XmlValue @field
}
