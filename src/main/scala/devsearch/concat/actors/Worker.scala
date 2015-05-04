package devsearch.concat.actors

import java.io._
import java.nio.file.{Paths, Files, Path}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import devsearch.concat.Utils
import devsearch.concat.actors.Coordinator._
import devsearch.concat.actors.Worker._
import org.apache.commons.compress.archivers.{ArchiveOutputStream, ArchiveStreamFactory}
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.compressors.{CompressorOutputStream, CompressorStreamFactory}
import org.apache.commons.compress.utils.IOUtils

/**
 * The worker actor is the one in charge of creating the large
 * files from smaller ones.
 *
 * @param master The actor to which it has to request new files
 */
class Worker(master: ActorRef) extends Actor with ActorLogging {

  /* The stream we are currently writing in */
  var currentStream = Option.empty[ArchiveOutputStream]
  var currentBlob = Option.empty[Path]

  /* The number of bytes written so far in the blob*/
  var bytesWritten = 0L

  /* The number of bytes we have seen and processed */
  var bytesSeen = 0L
  var bytesProcessed = 0L

  def receive = {
    /* Start to work */
    case Begin => master ! BlobRequest

    /* New blob to be created, it will contain the concatenation of many files */
    case BlobResponse(file) => {
      assert(currentStream == None)
      currentBlob = Some(file)

      bytesWritten = 0
      val out = new BufferedOutputStream(Files.newOutputStream(file))
      val tarOut = new ArchiveStreamFactory().createArchiveOutputStream(ArchiveStreamFactory.TAR, out)
      currentStream = Some(tarOut)
      master ! RepoRequest
    }

    /* One single file to append to the current blob */
    case RepoResponse(file, relativePath) =>
      val correctedPath = if (relativePath.endsWith(".tar")) relativePath.dropRight(4) else relativePath
      assert(currentStream.isDefined)
      val tarOut = currentStream.get

      val sizes = Utils.walkFiles(file) { fileEntry =>
        val isNormalFile = !fileEntry.isDirectory && !fileEntry.isSymbolicLink
        val isReasonableSize = fileEntry.size < Utils.maxFileSize

        /** This lazy is important because is TextFile might read the whole file in memory */
        lazy val isTextFile = Utils.isTextFile(fileEntry.inputStream)
        val size = fileEntry.size

        if (isNormalFile && isReasonableSize && isTextFile) {
          val entry = new TarArchiveEntry(Paths.get(correctedPath, fileEntry.relativePath).toString)
          entry.setSize(size)
          tarOut.putArchiveEntry(entry)
          IOUtils.copy(fileEntry.inputStream, tarOut)
          tarOut.closeArchiveEntry()
          (size, size)
        } else {
          (0L, size)
        }
      }

      val (seen, processed) = sizes.foldLeft((0L, 0L)) { (b, a) => (b._1 + a._1, b._2 + a._2) }
      bytesSeen += seen
      bytesProcessed += processed

      bytesWritten += processed
      if (bytesWritten >= Utils.maxFileSize) {
        tarOut.close()
        currentStream = None
        log.info(s"Finished with blob : ${currentBlob.get}")
        currentBlob = None
        master ! BlobRequest
      } else {
        master ! RepoRequest
      }



    /* No more files, end what you are doing and send finished message */
    case Shutdown => {
      currentStream.foreach {
        _.close()
      }
      log.info(s"Closing last blob : ${currentBlob.get}")
      sender ! Finished(bytesSeen, bytesProcessed)
    }
  }
}

object Worker {
  def props(reader: ActorRef) = Props(new Worker(reader))

  case object RepoRequest

  case object BlobRequest

  case object Begin

  case class Finished(bytesSeen: Long, bytesProcessed: Long)


}
