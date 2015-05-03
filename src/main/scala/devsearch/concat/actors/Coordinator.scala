package devsearch.concat.actors

import java.io.{File, IOException}
import java.nio.file.{Files, InvalidPathException}

import akka.actor.{Actor, ActorLogging, Props}
import devsearch.concat.actors.Coordinator.{BlobResponse, FileResponse, _}
import devsearch.concat.actors.Worker._
import org.apache.tika.Tika
import org.apache.tika.detect.TextDetector

/**
 * The coordinator is in charge of listing the files in the input directory and
 * distribute them to the Workers.
 *
 * @param langFolder The input folder where to find source files
 * @param outputFolder  The output folder where to output the big blobs
 * @param numWorkers The number of workers that it will have to coordinate
 */
case class Coordinator(langFolder: File, outputFolder: File, numWorkers: Int) extends Actor with ActorLogging {

  var files = Coordinator.listFilesRec(langFolder)
  var currentBlobNum = 0
  var numFinished = 0
  val blobSize = 640L << 20 // 640 Mb

  /** Text files bigger than 120Mb are probably not source code */
  val maxSize = 120L << 20 // 120Mb

  def receive = {
    /* Worker is done doing its work */
    case Finished => {
      numFinished += 1
      if (numFinished == numWorkers) {
        context.system.shutdown()
      }
    }

    /* Send next file to worker */
    case FileRequest => files match {
      case head #:: tail =>
        files = tail

        if(head.length > maxSize) {
          log.warning(s"Dropping file $head because it is suspiciously large")

          /* Resend request */
          self.!(FileRequest)(sender)
        } else {
          sender ! FileResponse(head, Coordinator.getRelativePath(head, langFolder))
        }
      /* If there are no more files, shutdown workers */
      case _ => sender ! Shutdown
    }

    /* Send next available blob to worker */
    case BlobRequest => {
      currentBlobNum += 1
      val blobName = "part-%05d.tar".format(currentBlobNum)
      sender ! BlobResponse(new File(outputFolder, blobName), blobSize)
    }
  }
}

object Coordinator {
  def props(langFolder: File, outputFolder: File, numWorkers: Int) =
    Props(new Coordinator(langFolder, outputFolder, numWorkers))

  case class FileResponse(file: File, relativeParentPath : String)

  case class BlobResponse(file: File, blobSize: Long)

  case object Shutdown

  /**
   * Checks whether a file is a text file.
   *
   * @param file The file to check
   * @return true if file is a text file
   */
  def isTextFile(file: File): Boolean = try {
    val contentType = Option(new Tika(new TextDetector()).detect(file))
    contentType.map(_.contains("text")).getOrElse(false)
  } catch {
    case o : IOException =>
      Console.err.println(s"Can't open $file to check that!")
      false
  }

  /**
   * Defines what is a good file, that is one that we want to include in our bigger files
   *
   * @param file The file that we want to test
   * @return true if file is a text file not hidden and not a link
   */
  def isGoodFile(file: File): Boolean = try {
    lazy val hidden = file.isHidden
    lazy val link = Files.isSymbolicLink(file.toPath)
    lazy val text = isTextFile(file)
    !hidden && !link && (file.isDirectory || text)
  } catch {
    case e : InvalidPathException =>
      Console.err.println(s"Can't convert $file to path, malformed input or invalid characters!")
      false
  }

  /** List all the files in a repository recursively
    *
    * @param repo The repository folder
    *
    */
  def listFilesRec(repo: File): Stream[File] = {

    def recScan(toScan: Stream[File]): Stream[File] = toScan match {
      case f #:: fs =>
        val children = f.listFiles.toSeq.filter(isGoodFile)
        val (folders, files) = children.partition(_.isDirectory)
        files.toStream #::: recScan(folders.toStream #::: fs)
      case _ => Stream.empty[File]
    }

    recScan(Stream(repo))
  }


  def getRelativePath(file : File, langFolder: File): String = {
    val rootFolder = langFolder.getParentFile
    rootFolder.toURI.relativize(file.toURI).getPath
  }
}