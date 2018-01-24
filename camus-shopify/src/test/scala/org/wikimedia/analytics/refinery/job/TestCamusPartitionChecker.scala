package org.wikimedia.analytics.refinery.job

import java.io.File
import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.reflect.io.{Directory}


class TestCamusPartitionChecker extends FlatSpec with Matchers with BeforeAndAfterEach {

  val camusHistoryTestFolder = "../camus-shopify/src/test/resources/camus-test-data"
  val failedRunFolder = "2015-08-15-17-52-01"
  val noHourRunFolder = "2015-09-29-15-20-08"
  val hourSpanRunFolder = "2015-10-02-08-00-07"
  val wrongFolder = "wrong-folder"
  var tmpDir:File = null

  override def beforeEach(): Unit = {
    CamusPartitionChecker.props.clear()
  }

  override def afterEach(): Unit = {
    if (tmpDir != null && tmpDir.exists()) {
      val d = new Directory(tmpDir)
      d.deleteRecursively()
    }
  }

  "A CamusChecker" should "find hours in between timestamps" in {
    val t1: Long = 1443428181000L // 2015-09-28T08:16:21  UTC
    val t2: Long = 1443436242000L // 2015-09-28T10:30:42  UTC

    val hours = CamusPartitionChecker.finishedHoursInBetween(t1, t2)
    val expectedHours = Seq((2015, 9, 28, 8), (2015, 9, 28, 9))
    hours should equal (expectedHours)
  }

  "A CamusChecker" should "find hours no hours in between inversed timestamps" in {
    val t1: Long = 1443428181000L // 2015-09-28T10:16:21
    val t2: Long = 1443436242000L // 2015-09-28T12:30:42

    val hoursEmpty = CamusPartitionChecker.finishedHoursInBetween(t2, t1)
    hoursEmpty should equal (Seq.empty)
  }

  it should "compute partition directory" in {
    val base = "/test/base/folder"
    val topic = "topic"
    val (year, month, day, hour) = (2015, 9, 28, 1)

    val partitionDir = CamusPartitionChecker.partitionDirectory(base, topic, year, month, day, hour)
    val expectedDir = "/test/base/folder/topic/2015/09/28/01"

    partitionDir should equal(expectedDir)
  }

  it should "replace '_' with '.' when computing partition directory" in {
    val base = "/test/base/folder"
    val topic = "big_topic_name"
    val (year, month, day, hour) = (2015, 9, 28, 1)

    val partitionDir = CamusPartitionChecker.partitionDirectory(base, topic, year, month, day, hour)
    val expectedDir = "/test/base/folder/big.topic.name/2015/09/28/01"

    partitionDir should equal(expectedDir)
  }

  it should "fail computing partition directory if no base" in {
    val base = null
    val topic = "topic"
    val (year, month, day, hour) = (2015, 9, 28, 1)

    intercept[IllegalArgumentException] {
      CamusPartitionChecker.partitionDirectory(base, topic, year, month, day, hour)
    }
  }

  it should "fail computing partition directory if no topic" in {
    val base = "/test/base/folder"
    val topic = null
    val (year, month, day, hour) = (2015, 9, 28, 1)

    intercept[IllegalArgumentException] {
      CamusPartitionChecker.partitionDirectory(base, topic, year, month, day, hour)
    }
  }

  it should "get topics and hours in a camus-run folder with whitelist" in {
    val folder: String = camusHistoryTestFolder + "/" + hourSpanRunFolder
    val path: Path = new Path(folder)

    // correct Whitelist, no blacklist --> Should work, one topic in historical data needs to be left aside
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.WHITELIST_TOPICS,
      "webrequest_maps,webrequest_text,webrequest_upload,webrequest_misc")

    val topicsAndHours = CamusPartitionChecker.getTopicsAndHoursToFlag(path)

    topicsAndHours.size should equal (4)
    // Some topics have hour to flag
    for ((t, o) <- topicsAndHours)
      if (! t.equals("webrequest_maps")) o.size should equal (1)
      else o.size should equal (0)
  }

  it should "get topics and hours in a camus-run folder with blacklist" in {
    val folder: String = camusHistoryTestFolder + "/" + hourSpanRunFolder
    val path: Path = new Path(folder)

    // No whitelist, correct blacklist --> Should work, one topic in historical data needs to be left aside
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.BLACKLIST_TOPICS,
      ".*_bits,.*_test")

    val topicsAndHours = CamusPartitionChecker.getTopicsAndHoursToFlag(path)

    topicsAndHours.size should equal (5)
    // Some topics have hour to flag
    for ((t, o) <- topicsAndHours)
      if (! t.equals("webrequest_maps")) o.size should equal (1)
      else o.size should equal (0)
  }

  it should "get topics and hours in a camus-run folder with whitelist with no hours to flag" in {
    val folder: String = camusHistoryTestFolder + "/" + noHourRunFolder
    val path: Path = new Path(folder)

    // correct Whitelist/blacklist config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.WHITELIST_TOPICS,
      "webrequest_maps,webrequest_text,webrequest_upload,webrequest_misc")

    val topicsAndHours = CamusPartitionChecker.getTopicsAndHoursToFlag(path)

    topicsAndHours.size should equal (4)
    // No hours to flag
    for ((t, o) <- topicsAndHours)
      o.size should equal (0)
  }

  it should "get correct hours to flag" in {
    val folder: String = camusHistoryTestFolder + "/" + hourSpanRunFolder
    val path: Path = new Path(folder)

    // correct Whitelist/blacklist config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.WHITELIST_TOPICS,
      "webrequest_maps,webrequest_text,webrequest_upload,webrequest_misc")

    val topicsAndHours = CamusPartitionChecker.getTopicsAndHoursToFlag(path)

    topicsAndHours.size should equal (4)
    topicsAndHours("webrequest_maps").size should equal (0)
    topicsAndHours("webrequest_text").toList should equal (List((2015, 10, 2, 7)))
    topicsAndHours("webrequest_upload").toList should equal (List((2015, 10, 2, 7)))
    topicsAndHours("webrequest_misc").toList should equal (List((2015, 10, 2, 7)))
  }

  it should "get correct hours to flag with a delay window" in {
    val folder: String = camusHistoryTestFolder + "/" + hourSpanRunFolder
    val path: Path = new Path(folder)

    // correct Whitelist/blacklist config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.WHITELIST_TOPICS,
      "webrequest_maps,webrequest_text,webrequest_upload,webrequest_misc")

    val topicsAndHours = CamusPartitionChecker.getTopicsAndHoursToFlag(path, 1L * 60 * 60 * 1000)

    topicsAndHours.size should equal (4)
    topicsAndHours("webrequest_maps").size should equal (0)
    topicsAndHours("webrequest_text").toList should equal (List((2015, 10, 2, 6)))
    topicsAndHours("webrequest_upload").toList should equal (List((2015, 10, 2, 6)))
    topicsAndHours("webrequest_misc").toList should equal (List((2015, 10, 2, 6)))
    // note the difference between previous and current test is that we move the previous stable offset by an hour also
    // this results in a full shift of hours to check
  }

  it should "doesn't fail getting topics and hours in an error camus-run folder" in {
    // Original test was checking for failures, this one makes sure it won't fail
    val folder: String = camusHistoryTestFolder + "/" + failedRunFolder
    val path: Path = new Path(folder)

    // correct Whitelist/blacklist config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.WHITELIST_TOPICS,
      "webrequest_maps,webrequest_text,webrequest_upload,webrequest_misc")

    CamusPartitionChecker.getTopicsAndHoursToFlag(path)
  }

  it should "write the file flag for a given partition hour" in {
    tmpDir = Files.createTempDirectory("testcamus").toFile
    val partitionFolder = "testtopic/2015/10/02/08"
    val d = new Directory(new File(tmpDir, partitionFolder))
    d.createDirectory()

    d.list shouldBe empty

    // correct partition base path config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.PARTITION_BASE_PATH, tmpDir.getAbsolutePath)

    CamusPartitionChecker.flagFullyImportedPartitions("_TESTFLAG", false, Map("testtopic" -> Seq((2015, 10, 2, 8))))

    d.list should not be empty
    d.list.toSeq.map(_.toString()) should contain (tmpDir.getAbsolutePath() + "/testtopic/2015/10/02/08/_TESTFLAG")

  }

  it should "not overwrite the existing file flag for a given partition hour" in {
    tmpDir = Files.createTempDirectory("testcamus").toFile
    val partitionFolder = "testtopic/2015/10/02/08"
    val d = new Directory(new File(tmpDir, partitionFolder))
    d.createDirectory()

    d.list shouldBe empty

    // correct partition base path config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.PARTITION_BASE_PATH, tmpDir.getAbsolutePath)

    CamusPartitionChecker.flagFullyImportedPartitions("_TESTFLAG", false, Map("testtopic" -> Seq((2015, 10, 2, 8))))

    val fileList = d.list.toList
    fileList should not be empty
    fileList.map(_.toString()) should contain (tmpDir.getAbsolutePath() + "/testtopic/2015/10/02/08/_TESTFLAG")

    // get modification time of the flag
    val flagTime = fileList.find(_.toString().endsWith("_TESTFLAG")).get.lastModified

    // watermark again
    CamusPartitionChecker.flagFullyImportedPartitions("_TESTFLAG", false, Map("testtopic" -> Seq((2015, 10, 2, 8))))

    // the modification time of the flag should not change
    val fileList2 = d.list.toList
    val flagTime2 = fileList.find(_.toString().endsWith("_TESTFLAG")).get.lastModified

    flagTime shouldEqual flagTime2
  }

  it should "doesn't fail writing the file flag if the given partition hour folder doesn't exist" in {
    // Original test was checking for failures, this one makes sure it won't fail
    tmpDir = Files.createTempDirectory("testcamus").toFile();
    val partitionFolder = "testtopic/hourly/2015/10/02/08"
    val d = new Directory (new File(tmpDir, partitionFolder))

    // correct partition base path config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.PARTITION_BASE_PATH, tmpDir.getAbsolutePath())

    CamusPartitionChecker.flagFullyImportedPartitions("_TESTFLAG", false, Map("testtopic" -> Seq((2015, 10, 2, 8))))
  }

}
