package devsearch.concat

import java.io._
import java.nio.file.{Paths, Files, Path}

import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.tar.{TarArchiveInputStream, TarArchiveEntry}
import org.apache.commons.compress.utils.IOUtils
import org.apache.tika.Tika
import org.apache.tika.detect.TextDetector

/**
 * Created by dengels on 04/05/15.
 */
object Utils {

  import scala.Console._

  /** Size of one blob we build */
  val blobSize = 640L << 20 // 640 Mb

  /** Text files bigger than 2Mb are probably not source code */
  val maxFileSize = 2L << 20 // 2Mb


  /**
   * Checks whether a stream contains text data
   *
   * @param is The input stream to read from
   * @return true if is contains text data
   */
  def isTextFile(is: InputStream): Boolean = try {
    val contentType = Option(new Tika(new TextDetector()).detect(is))
    contentType.exists(_.contains("text"))
  } catch {
    case o: IOException =>
      err.println(s"Input Stream could not be read to ")
      false
  }

  /**
   * Checks whether a file contains text data
   *
   * @return true if file is a text file
   */
  def isTextFile(file: Path): Boolean = try {
    isTextFile(new FileInputStream(file.toFile))
  } catch {
    case e: FileNotFoundException =>
      err.println(s"Could not find file $file")
      false
  }

  /** List all the files in a folder recursively */
  def listFilesRec(folder: Path): Stream[Path] = {

    def recScan(toScan: Stream[Path]): Stream[Path] = toScan match {
      case f #:: fs =>
        val children = f.toFile.listFiles.toSeq.map(_.toPath)
        val goodFiles = children.filterNot(p => Files.isHidden(p) || Files.isSymbolicLink(p))
        val (folders, files) = goodFiles.partition(Files.isDirectory(_))
        files.toStream #::: recScan(folders.toStream #::: fs)
      case _ => Stream.empty[Path]
    }

    recScan(Stream(folder))
  }

  /** Returns all the repos contained in the repository root
    *
    * repos are organized by language, then owner, then reponame
    *
    * repos can be either folder, or tarballs
    */
  def getRepoPaths(repoRoot: Path): Stream[Path] = {
    val languages = repoRoot.toFile.listFiles.toStream

    languages.filterNot(_.isDirectory).foreach { file =>
      err.println(s"Found regular file $file when expecting language directory")
    }


    val owners = languages.filter(_.isDirectory).flatMap(_.listFiles.toStream)

    owners.filterNot(_.isDirectory).foreach { file =>
      err.println(s"Found regular file $file when expecting owner directory")
    }

    owners.filter(_.isDirectory).flatMap(_.listFiles.toStream.map(_.toPath))
  }

  trait FileEntry {
    def relativePath: String

    @throws[IOException]
    def inputStream: InputStream

    @throws[IOException]
    def size: Long
  }

  /** Whether the repo is a directory or has been put in a tar ball */
  def walkFiles[T](repo: Path)(processEntry: FileEntry => T): TraversableOnce[T] = {
    if (Files.isDirectory(repo)) {
      listFilesRec(repo).map { p =>

        lazy val is = new BufferedInputStream(Files.newInputStream(p))
        val entry = new FileEntry {

          override def size: Long = Files.size(p)

          override def relativePath: String = repo.relativize(p).toString

          override def inputStream: InputStream = is
        }

        val res = processEntry(entry)
        try {
          is.close()
        } catch {
          case e: IOException => err.println(s"Could not close stream for entry $p")
        }

        res
      }
    } else if (repo.toString.endsWith(".tar")) {
      val is = new BufferedInputStream(Files.newInputStream(repo))
      val tarInput = new ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.TAR, is).asInstanceOf[TarArchiveInputStream]
      var entry: TarArchiveEntry = tarInput.getNextTarEntry
      var results = Seq.empty[T]
      while (entry != null) {
        /* Since the tarInput does not support marking, we have to cheat */
        lazy val is = new BufferedInputStream(tarInput)
        val fileEntry = new FileEntry {
          override def size: Long = entry.getSize

          override def relativePath: String = entry.getName.dropWhile(_ != '/').tail

          override def inputStream: InputStream = is
        }
        /** Filter symbolic files and directories and hidden files */
        val isHidden = scala.collection.JavaConversions.asScalaIterator(Paths.get(fileEntry.relativePath).iterator).exists(_.toString.startsWith("."))
        if (!entry.isSymbolicLink && !entry.isDirectory && !isHidden) {
          val res = processEntry(fileEntry)
          results = results :+ res
        }

        entry = tarInput.getNextTarEntry
      }
      tarInput.close()
      results
    } else {
      err.println(s"$repo does not appear to be a folder or tarball repository")
      Iterator.empty
    }

  }
}
