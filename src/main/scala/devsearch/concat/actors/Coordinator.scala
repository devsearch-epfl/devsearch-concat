package devsearch.concat.actors

import java.nio.file.{ Files, InvalidPathException, Path }

import akka.actor.{ Actor, ActorLogging, Props }
import devsearch.concat.Utils
import devsearch.concat.actors.Coordinator._
import devsearch.concat.actors.Worker._

/**
 * The coordinator is in charge of listing the files in the input directory and
 * distribute them to the Workers.
 *
 * @param repoRoot The input folder where to find source files
 * @param outputFolder  The output folder where to output the big blobs
 * @param numWorkers The number of workers that it will have to coordinate
 */
case class Coordinator(repoRoot: Path, outputFolder: Path, numWorkers: Int) extends Actor with ActorLogging {

  import Utils._

  /** The repos we have to process */
  var repos = getRepoPaths(repoRoot)
  var currentBlobNum = 0

  /** Total number of bytes in the root folder */
  var totalBytesSeen = 0L

  /** Total number of bytes that were added to the blobs */
  var totalBytesProcesses = 0L

  /** Number of workers that have finished working */
  var workerFinished = 0L

  log.info(s"Starting up coordinator with $numWorkers worker on root folder : $repoRoot and ouput folder $outputFolder")

  override def receive: PartialFunction[Any, Unit] = {
    /* Worker is done doing its work */
    case Finished(bytesSeen, bytesProcessed) =>
      totalBytesSeen += bytesSeen
      totalBytesProcesses += bytesProcessed
      workerFinished += 1
      if (workerFinished == numWorkers) {
        log.info(s"Shutting down system! Processed ${totalBytesProcesses}B out of ${totalBytesSeen}B")
        context.system.shutdown()
      }

    /* Send next repo to worker */
    case RepoRequest => repos match {
      case repo #:: tail =>
        repos = tail

        sender ! RepoResponse(repo, getRelativePath(repo, repoRoot))

      /* If there are no more files, shutdown workers */
      case _ => sender ! Shutdown
    }

    /* Send next available blob to worker */
    case BlobRequest =>
      currentBlobNum += 1
      val blobName = "part-%05d.tar".format(currentBlobNum)
      sender ! BlobResponse(Files.createFile(outputFolder.resolve(blobName)))

  }
}

object Coordinator {
  def props(langFolder: Path, outputFolder: Path, numWorkers: Int): Props =
    Props(new Coordinator(langFolder, outputFolder, numWorkers))

  case class RepoResponse(file: Path, relativeParentPath: String)

  case class BlobResponse(file: Path)

  case object Shutdown

  /**
   * Defines what is a good file, that is one that we want to include in our bigger files
   *
   * @param file The file that we want to test
   * @return true if file is a text file not hidden and not a link
   */
  def isGoodFile(file: Path): Boolean = try {
    lazy val hidden = Files.isHidden(file)
    lazy val link = Files.isSymbolicLink(file)
    lazy val text = Utils.isTextFile(file)
    !hidden && !link && (Files.isDirectory(file) || text)
  } catch {
    case e: InvalidPathException =>
      Console.err.println(s"Can't convert $file to path, malformed input or invalid characters!")
      false
  }

  def getRelativePath(file: Path, repoRoot: Path): String = {
    repoRoot.relativize(file).toString
  }
}
