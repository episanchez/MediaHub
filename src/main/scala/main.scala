import java.nio.file.Paths

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.amqp.scaladsl.AmqpSink
import akka.stream.alpakka.amqp.{AmqpLocalConnectionProvider, AmqpWriteSettings, QueueDeclaration, WriteMessage}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult, ThrottleMode}
import akka.util.ByteString
import main.Titles

import scala.concurrent.Future

object main {
  case class Titles(tconst: String, titleType: String, primaryTitle: String, originalTitle: String, isAdult: String, startYear: String, endYear: String, runtimeMinutes: String, genres: String){

    }
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("MyAkkaSystem")
    implicit val materializer = ActorMaterializer()

    def createTitles(titles: Array[String]) = {
      Titles(
        titles(0),
        titles(1),
        titles(2),
        titles(3),
        titles(4),
        titles(5),
        titles(6),
        titles(7),
        titles(8)
      )
    }

    val rabbit = AmqpLocalConnectionProvider
    val queueName = "movie"
    val queueDeclaration = QueueDeclaration(queueName)

    val sink : Sink[ByteString, Future[Done]] =
      AmqpSink.simple(
        AmqpWriteSettings(rabbit)
          .withRoutingKey(queueName)
          .withDeclaration(queueDeclaration)
      )

    FileIO.fromPath(Paths.get("data.tsv"))
      .via(Framing.delimiter(ByteString("\n"), 1024, false).map(_.utf8String))
      .map(x => createTitles(x.split("\t")))
      .filter(title => title.titleType == "movie")
      .filter(title =>
        title.originalTitle.contains("Comedy") ||
        title.primaryTitle.contains("Comedy")
      )
      .map(x => ByteString(x.originalTitle))
      .to(sink)
      .run()
  }
}
