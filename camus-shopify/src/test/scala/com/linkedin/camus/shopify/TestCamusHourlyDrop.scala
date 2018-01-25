package com.linkedin.camus.shopify

import org.apache.hadoop.fs.Path

class TestCamusHourlyDrop extends CamusTest {
  val topicsDrop = "webrequest.text/2015/10/02/06"
  val dataFile = "webrequest.text.11.103.3312.22301266.1516665600000.gz"

  override def beforeEach(): Unit = {
    super.beforeEach()
    createTopicPartition(topicsDrop)
    dropFile(topicsDrop, dataFile)
    setFlag(topicsDrop)
  }

  test("flagPath") {
    val drop = new CamusHourlyDrop(new Path(topicsDrop), fs)
    assert(drop.flagPath.toString.equals(topicsDrop + "/_IMPORTED"))
  }

  test("hasFlag") {
    val normal = new CamusHourlyDrop(topicsDropPath(topicsDrop), fs)
    assert(normal.hasFlag)

    removeFlag(topicsDrop)
    val unflagged = new CamusHourlyDrop(topicsDropPath(topicsDrop), fs)
    assert(!unflagged.hasFlag)
  }

  test("flagWrittenAt") {
    fs.setTimes(flagPath(topicsDrop), 1000000000000L, 0)
    val drop = new CamusHourlyDrop(topicsDropPath(topicsDrop), fs)

    assertResult(1000000000000L) { drop.flagWrittenAt }
  }

  test("lastFileWrittenAt") {
    fs.setTimes(dataPath(topicsDrop, dataFile), 1000000000000L, 0)
    val drop = new CamusHourlyDrop(topicsDropPath(topicsDrop), fs)

    assertResult(1000000000000L) { drop.lastFileWrittenAt }
  }

  test("isFlagViolated") {
    val drop = new CamusHourlyDrop(topicsDropPath(topicsDrop), fs)

    fs.setTimes(dataPath(topicsDrop, dataFile), 1000000000000L, 0)
    fs.setTimes(flagPath(topicsDrop), 1300000000000L, 0)
    assert(!drop.isFlagViolated)

    fs.setTimes(dataPath(topicsDrop, dataFile), 1400000000000L, 0)
    assert(drop.isFlagViolated)
  }

  test("violations: single file") {
    val drop = new CamusHourlyDrop(topicsDropPath(topicsDrop), fs)
    fs.setTimes(flagPath(topicsDrop), 1300000000000L, 0)
    fs.setTimes(dataPath(topicsDrop, dataFile), 1400000000000L, 0)

    val violations = drop.violations
    assert(violations.size == 1)

    val (path, count) = violations.head
    assert(path.replace("file:", "") == dataPath(topicsDrop, dataFile).toString)
    assert(count == 3312)
  }

  test("violations: multiple files") {
    val dataFile2 = "webrequest.text.12.103.40.22211266.1516665600001.gz"
    val dataFile3 = "webrequest.text.12.103.2.22211266.1516665600001.gz"
    dropFile(topicsDrop, dataFile2)
    dropFile(topicsDrop, dataFile3)

    val drop = new CamusHourlyDrop(topicsDropPath(topicsDrop), fs)
    fs.setTimes(dataPath(topicsDrop, dataFile), 1200000000000L, 0)
    fs.setTimes(flagPath(topicsDrop), 1300000000000L, 0)
    fs.setTimes(dataPath(topicsDrop, dataFile2), 1400000000000L, 0)
    fs.setTimes(dataPath(topicsDrop, dataFile3), 1400000000000L, 0)

    val violations = drop.violations
    assert(violations.size == 2)

    val paths = violations.map(_._1.replace("file:", ""))
    assert(paths.toSet == Set(dataPath(topicsDrop, dataFile2).toString, dataPath(topicsDrop, dataFile3).toString))

    val count = violations.map(_._2).sum
    assert(count == 42)
  }
}
