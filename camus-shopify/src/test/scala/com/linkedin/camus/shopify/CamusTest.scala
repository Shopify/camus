package com.linkedin.camus.shopify

import java.io.File
import java.nio.file.Files
import java.util.Properties

import scala.reflect.io.Directory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class CamusTest extends FunSuite with Matchers with BeforeAndAfterEach {
  val fs: FileSystem = FileSystem.get(new Configuration())
  val camusHistoryTestFolder: String = "../camus-shopify/src/test/resources/camus-test-data"
  val props: Properties = new Properties()
  props.setProperty("etl.execution.history.path", camusHistoryTestFolder)

  val flag: String = "_IMPORTED"
  private var tmpDir: File = null


  override def beforeEach(): Unit = {
    tmpDir = Files.createTempDirectory("testcamusdest").toFile

    val camusDestinationPath = tmpDir.getAbsolutePath
    props.setProperty("etl.destination.path", camusDestinationPath)

  }

  override def afterEach(): Unit = {
    if (tmpDir != null && tmpDir.exists()) {
      val d = new Directory(tmpDir)
      d.deleteRecursively()
    }
  }

  def setFlag(topicPartition: String): Unit = {
    val flagFile = new File(topicsDropPath(topicPartition).toString, flag)
    flagFile.createNewFile()
  }

  def removeFlag(topicPartition: String): Unit = {
    val flagFile = new File(topicsDropPath(topicPartition).toString, flag)
    flagFile.delete()
  }

  def topicsDropPath(topicPartition: String): Path  = {
    new Path(s"${tmpDir.getAbsolutePath}/$topicPartition")
  }

  def dataPath(topicPartition: String, filename: String): Path  = {
    new Path(s"${tmpDir.getAbsolutePath}/$topicPartition/$filename")
  }

  def flagPath(topicPartition: String): Path  = {
    new Path(s"${tmpDir.getAbsolutePath}/$topicPartition/$flag")
  }

  def createTopicPartition(topicPartition: String): Unit = {
    val newPartitionForTopic = new Directory(new File(tmpDir, topicPartition))
    newPartitionForTopic.createDirectory()
  }

  def dropFile(topicPartition: String, fileName: String): Unit = {
    val dataFile = new File(topicsDropPath(topicPartition).toString, fileName)
    dataFile.createNewFile()
  }
}