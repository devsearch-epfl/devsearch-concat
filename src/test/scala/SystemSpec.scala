import java.nio.file.{Path, Files}
import java.util
import java.util.function.IntFunction

import actors.Worker
import de.svenjacobs.loremipsum.LoremIpsum
import org.scalatest.{Matchers, WordSpecLike}

import scala.util.Random

/**
 * Created by dengels on 03/05/15.
 */
class SystemSpec extends WordSpecLike with Matchers {

  "The system" should {

    "Have the correct size for small text files" in {

      val loremIpsum = new LoremIpsum()
      val rootDir = Files.createTempDirectory("rootDir")
      val langDir = rootDir.resolve("lang")
      Files.createDirectory(langDir)
      val files = for(i <- 1 to 10) yield {
        val file = langDir.resolve(s"file_$i")
        Files.createFile(file)
        val bytes = loremIpsum.getParagraphs(i).getBytes("UTF-8")
        Files.write(file, bytes)
      }
      
      val totalSize = files.map{file => Files.size(file)}.sum

      val resDir = Files.createTempDirectory("resDir")

      ParallelConcat.main(Array("-j", "1") ++ Array(rootDir, resDir).map(_.toAbsolutePath.toString))

      val blobs = Files.list(Files.list(resDir).findFirst.get).toArray(new IntFunction[Array[Path]] {
        override def apply(value: Int): Array[Path] = new Array[Path](value)
      })

      blobs.size should equal(1)

      val resSize = Files.size(blobs(0))

      resSize should be > totalSize

    }


  }
}
