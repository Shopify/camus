package com.linkedin.camus.shopify


class TestLateArrivingDataMonitor extends CamusTest {
  val window = 1000 * 60 * 60 * 2 // 2 hours
  props.setProperty("etl.execution.history.path", camusHistoryTestFolder)

  val topicsDrop = "webrequest.text/2015/10/02/06"
  val dataFile = "webrequest.text.11.103.3312.22301266.1516665600000.gz"

  override def beforeEach(): Unit = {
    super.beforeEach()
    createTopicPartition(topicsDrop)
    dropFile(topicsDrop, dataFile)
    setFlag(topicsDrop)
  }

  test("checkDropsInWindow no late-arriving data") {
    fs.setTimes(dataPath(topicsDrop, dataFile), 1000000000000L, 0)
    fs.setTimes(flagPath(topicsDrop), 1300000000000L, 1)

    val violations = LateArrivingDataMonitor.checkDropsInWindow(props, fs, window)
    assert(!violations)
  }

  test("checkDropsInWindow with late-arriving data") {
    fs.setTimes(flagPath(topicsDrop), 1000000000000L, 0)
    fs.setTimes(dataPath(topicsDrop, dataFile), 1300000000000L, 0)

    val violations = LateArrivingDataMonitor.checkDropsInWindow(props, fs, window)
    assert(violations)
  }

}


