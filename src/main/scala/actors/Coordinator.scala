package actors

import java.io.File
import java.nio.file.Files

import actors.Coordinator.{BlobResponse, FileResponse, Shutdown}
import actors.Worker.{Finished, FileRequest, BlobRequest}
import akka.actor.{Actor, Props}
import org.apache.tika.detect.TextDetector
import org.apache.tika.Tika

/**
 * The coordinator is in charge of listing the files in the input directory and
 * distribute them to the Workers.
 *
 * @param langFolder The input folder where to find source files
 * @param outputFolder  The output folder where to output the big blobs
 * @param numWorkers The number of workers that it will have to coordinate
 */
case class Coordinator(langFolder: File, outputFolder: File, numWorkers: Int) extends Actor {

  var files = Coordinator.listFilesRec(langFolder)
  var currentBlobNum = 0
  var numFinished = 0
  val blobSize = (64L * 10L) << 20 // 640 Mb

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
        sender ! FileResponse(head)
      /* If there are no more files, shutdown workers */
      case _ => sender ! Shutdown
    }

    /* Send next available blob to worker */
    case BlobRequest => {
      currentBlobNum += 1
      val blobName = "part-%05d".format(currentBlobNum)
      sender ! BlobResponse(new File(outputFolder, blobName), blobSize)
    }
  }
}

object Coordinator {
  def props(langFolder: File, outputFolder: File, numWorkers: Int) =
    Props(new Coordinator(langFolder, outputFolder, numWorkers))

  case class FileResponse(file: File)

  case class BlobResponse(file: File, blobSize: Long)

  case object Shutdown

  /**
   * Checks whether a file is a text file.
   *
   * @param file The file to check
   * @return true if file is a text file
   */
  def isTextFile(file: File): Boolean = {
    val contentType = Option(new Tika(new TextDetector()).detect(file))
    contentType.map(_.contains("text")).getOrElse(false)
  }

  /**
   * Defines what is a good file, that is one that we want to include in our bigger files
   *
   * @param file The file that we want to test
   * @return true if file is a text file not hidden and not a link
   */
  def isGoodFile(file: File): Boolean = {
    lazy val hidden = file.isHidden
    lazy val link = Files.isSymbolicLink(file.toPath)
    lazy val text = isTextFile(file)
    !hidden && !link && (file.isDirectory || text)
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
}