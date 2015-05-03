package actors

import java.io._

import actors.Coordinator.{FileResponse, BlobResponse, Shutdown}
import actors.Worker.{Finished, FileRequest, Begin, BlobRequest}
import akka.actor.{ActorRef, Actor, ActorLogging, Props}
import org.kamranzafar.jtar.{TarEntry, TarOutputStream}

import scala.concurrent.Future

/**
 * The worker actor is the one in charge of creating the large
 * files from smaller ones.
 *
 * @param master The actor to which it has to request new files
 */
class Worker(master: ActorRef) extends Actor with ActorLogging {

  /* The stream we are currently writing in */
  var currentStream = Option.empty[TarOutputStream]
  var currentBlob = Option.empty[File]

  /* The maximum number of bytes we are allowed to write to the stream */
  var maxSize = 0L
  /* The number of bytes written so far */
  var bytesWritten = 0L

  /* Temporary file we have not written yet */
  var tempFile = Option.empty[FileResponse]

  def receive = {
    /* Start to work */
    case Begin => master ! BlobRequest

    /* New blob to be created, it will contain the concatenation of many files */
    case BlobResponse(file, size) => {
      assert(currentStream == None)
      // TODO: check output
      file.createNewFile()
      currentBlob = Some(file)
      maxSize = size
      bytesWritten = 0
      currentStream = Some(new TarOutputStream(new DataOutputStream(new FileOutputStream(file))))
      tempFile.map(self ! _).getOrElse(master ! FileRequest)
    }

    /* One single file to append to the current blob */
    case fr @ FileResponse(file, relativePath) => {
      val fileSize = file.length()


      if (fileSize + bytesWritten > maxSize) {
        /* The blob is full */
        tempFile = Some(fr)
        currentStream.foreach {
          _.close
        }
        currentStream = None
        log.info(s"Finished with ${currentBlob.get}")
        currentBlob = None
        master ! BlobRequest
      } else {
        /* Append the current file */
        bytesWritten += fileSize
        tempFile = None
        assert(currentStream.isDefined)
        currentStream.foreach { dos =>
          dos.putNextEntry(new TarEntry(file, relativePath));
          val is = new FileInputStream(file)
          Worker.copyBytes(is, dos)
          is.close()
        }
        master ! FileRequest
      }
    }

    /* No more files, end what you are doing and send finished message */
    case Shutdown => {
      currentStream.foreach {
        _.close()
      }
      log.info(s"Closing last blob : ${currentBlob.get}")
      sender ! Finished
    }
  }
}

object Worker {
  def props(reader: ActorRef) = Props(new Worker(reader))

  case object FileRequest

  case object BlobRequest

  case object Begin

  case object Finished

  /**
   * Copy bytes from one input stream to an outputstream
   *
   * @param in The input stream from which to read bytes
   * @param out The output stream to write bytes to
   */
  def copyBytes(in: InputStream, out: OutputStream): Unit = {
    val buffer = new Array[Byte](1024)
    var len = in.read(buffer)
    while (len != -1) {
      out.write(buffer, 0, len)
      len = in.read(buffer)
    }
  }

}
