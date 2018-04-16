package org.wikimedia.analytics.refinery.job

import scopt.OptionParser

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, SequenceFile}
import org.apache.log4j.Logger

import com.linkedin.camus.etl.kafka.common.EtlKey

case class Params(
    executionFolder: String = "",
    topic: String = "",
    partition: Long = -1,
    offset: Long = -1
)

/**
 * Replace an offset for a topic partition with a different offset.
 * New file will be written as patched-<original_file>.
 *
 * Known issues: cannot easily deal with patching multiple offsets in a single file.
 */
object CamusOffsetPatcher {

  val log: Logger = Logger.getLogger(CamusOffsetPatcher.getClass)

  var originalFile: Path = null
  var newFile: Path = null

  def getNewKeyPath(path: Path) = new Path(path.getParent, s"patched-${path.getName}")

  val argsParser = new OptionParser[Params]("Camus Offset Patcher") {
    head("Camus Offset Patcher", "")
    note("Patches the relevant offset file with new offset.")
    help("help") text "Prints this usage text"

    opt[String]('e', "execution-folder") required() valueName ("<path>") action { (x, p) =>
      p.copy(executionFolder = x)
    } text "Camus execution folder to patch."

    opt[String]('t', "topic") required() valueName ("<path>") action { (x, p) =>
      p.copy(topic = x)
    } text "Camus topic to patch."

    opt[String]('p', "partition") required() action { (x, p) =>
      p.copy(partition = x.toLong)
    } text "Camus partition to patch."

    opt[String]('o', "offset") required() action { (x, p) =>
      p.copy(offset = x.toLong)
    } text "Camus new offset to set for topic partition."
  }

  def main(args: Array[String]): Unit = {
    val fs = FileSystem.get(new Configuration())
    val camusReader = new CamusStatusReader(fs)


    argsParser.parse(args, Params()) match {
      case Some(params) =>
        log.info("Looking for files..")
        val files = camusReader.offsetsFiles(new Path(params.executionFolder))

        log.info("Fixing offsets..")
        fixOffsets(fs, camusReader, files, params.topic, params.partition, params.offset)

        log.info("Validating..")
        validate(camusReader, originalFile, newFile)
      case _  =>
        log.error("Could not parse parameters")
        System.exit(1)
    }
  }

  def validate(camusReader: CamusStatusReader, oldPath: Path, newPath: Path): Unit = {
    val oldKeys = camusReader.readEtlKeys(oldPath)
    val newKeys = camusReader.readEtlKeys(newPath)

    log.info("OLD FILE CONTENT")
    oldKeys.foreach(
      k => log.info(s"=> EtlKey: topic=${k.getTopic} part=${k.getPartition} offset=${k.getOffset}")
    )

    log.info("NEW FILE CONTENT")
    newKeys.foreach(
      k => log.info(s"=> New EtlKey: topic=${k.getTopic} part=${k.getPartition} offset=${k.getOffset}")
    )
  }

  def fixOffsets(
    fs: FileSystem,
    camusReader: CamusStatusReader,
    files: Seq[Path],
    topic: String,
    partition: Long,
    newOffset: Long): Unit = {

    files.foreach({
      file =>
        log.info(s"Inspecting $file")
        val keys = camusReader.readEtlKeys(file)
        val relevantKeys = keys.filter(k => k.getTopic == topic && k.getPartition == partition)

        // we found the relevant file
        if (relevantKeys.nonEmpty) {
          log.info(s"Found relevant key in $file")
          originalFile = file
          assert(relevantKeys.size == 1, "More than 1 key found")
          val badKey = relevantKeys.head
          // set the correct offset
          log.info(s"Setting offset to $newOffset for ${badKey.getTopic}:${badKey.getPartition}")
          badKey.setOffset(newOffset)

          // re-write all the keys to another file
          val newPath = getNewKeyPath(file)
          newFile = newPath
          log.info(s"Rewriting offsets to a new file: $newPath")

          val offsetWriter = SequenceFile.createWriter(fs, new Configuration(), newPath, classOf[EtlKey], classOf[NullWritable])
          keys.foreach({
            k => offsetWriter.append(k, NullWritable.get)
          })
          offsetWriter.close()
        }
    })
  }
}
