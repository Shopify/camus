package org.wikimedia.analytics.refinery.job

import java.io.FileInputStream
import java.util.Properties

import com.linkedin.camus.etl.kafka.CamusJob
import com.linkedin.camus.etl.kafka.common.EtlKey
import com.linkedin.camus.etl.kafka.mapred.{EtlInputFormat, EtlMultiOutputFormat}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{LogManager, Logger}
import org.joda.time.{Hours, DateTimeZone, DateTime}
import scopt.OptionParser
import com.github.nscala_time.time.Imports._

/**
  *
  * This is a modified version of the original class found in https://github.com/wikimedia/analytics-refinery-source
  * whose original author is Joseph Allemandou <joal@wikimedia.org>
  *
  * Class marking checking camus runs based on a camus.properties file.
  * It flags hdfs imported data for fully imported hours.
  *
  * command example (replace [*] with * in classpath - hack to prevent scala comment issue):
  * java -Dlog4j.configuration=file:///home/joal/code/log4j_console.properties \
  *      -cp "/home/joal/code/analytics-refinery-source/refinery-job/target/refinery-job-0.0.21-SNAPSHOT.jar:/usr/lib/spark/lib/[*]:/usr/lib/hadoop/[*]:/usr/lib/hadoop-hdfs/[*]:/usr/lib/hadoop/lib/[*]:/usr/share/java/[*]" \
  *      org.wikimedia.analytics.refinery.job.CamusPartitionChecker -c /home/joal/camus.test.import.properties
  */
object CamusPartitionChecker {

  val BLACKLIST_TOPICS = EtlInputFormat.KAFKA_BLACKLIST_TOPIC
  val WHITELIST_TOPICS = EtlInputFormat.KAFKA_WHITELIST_TOPIC
  val PARTITION_BASE_PATH = EtlMultiOutputFormat.ETL_DESTINATION_PATH
  val EARLIEST_OF_TIMES = 1388534400000L // Jan 1, 2014


  // Dummy values, to be set with configuration in main
  var fs: FileSystem = FileSystem.get(new Configuration)
  var camusReader: CamusStatusReader = new CamusStatusReader(fs)
  val props: Properties = new Properties
  val log: Logger = Logger.getLogger(CamusPartitionChecker.getClass)

  /**
   * Computes calendar hours happening between two timestamps. For instance
   * if  t1 =  = 2015-09-25 03:28:12
   * and t2 =  = 2015-09-25 06:55:32
   * Returned result is
   *      [(2015, 9, 25, 4), (2015, 9, 25, 5),(2015, 9, 25, 6)]
   *
   * @param t1 The first timestamp (oldest)
   * @param t2 The second timestamp (youngest)
   * @return the hours having happened between t1 and t2 in format (year, month, day, hour)
   */
  def finishedHoursInBetween(t1: Long, t2: Long): Seq[(Int, Int, Int, Int)] = {
    val oldestNextHour = new DateTime(t1 , DateTimeZone.UTC).hourOfDay.roundCeilingCopy
    val youngestPreviousHour = new DateTime(t2, DateTimeZone.UTC).hourOfDay.roundFloorCopy
    for (h <- 0 to Hours.hoursBetween(oldestNextHour, youngestPreviousHour).getHours ) yield {
      val fullHour: DateTime = oldestNextHour + h.hours - 1.hours
      (fullHour.year.get, fullHour.monthOfYear.get, fullHour.dayOfMonth.get, fullHour.hourOfDay.get)
    }
  }

  /**
    * In ShopifyPartitioner (which is part of our Camus import job), we replace underscores with dots for
    * out topic names. That was done in order to be backwards compatible without having to move data around.
    * This is here to make sure we can find the correct paths to add the flag.
    */
  def partitionDirectory(base: String, topic: String, year: Int, month: Int, day: Int, hour: Int): String = {
    if ((! StringUtils.isEmpty(base)) && (! StringUtils.isEmpty(topic))) {
      val dottedTopicName = topic.replaceAll("_", "\\.")
      f"${base}%s/${dottedTopicName}%s/${year}%04d/${month}%02d/${day}%02d/${hour}%02d"
    }
    else
      throw new IllegalArgumentException("Can't make partition directory with empty base or topic.")
  }

  /** Compute complete hours imported on a camus run by topic. Throws an IllegalStateException if
    * the camus run state is not correct (missing topics or import-time not moving)
    *
    * @param camusRunPath the camus run Path folder to use
    * @return a map of topic -> Seq[(year, month, day, hour)]
    */
  def getTopicsAndHoursToFlag(camusRunPath: Path, delayMilli: Long = 0L, backfill: Boolean = false): Map[String, Seq[(Int, Int, Int, Int)]] = {
    var whitelist = props.getProperty(WHITELIST_TOPICS, ".*")
    if (whitelist.isEmpty) whitelist = ".*"

    // Empty Whitelist means all --> default to .* regexp
    val topicsWhitelist = "(" + whitelist.replaceAll(" *, *", "|") + ")"
    // Empty Blacklist means no blacklist --> Default to empty string
    val topicsBlacklist = "(" + props.getProperty(BLACKLIST_TOPICS, "").replaceAll(" *, *", "|") + ")"

    val currentOffsets: Seq[EtlKey] = camusReader.readEtlKeys(camusReader.offsetsFiles(camusRunPath))
    val previousOffsets: Seq[EtlKey] = camusReader.readEtlKeys(camusReader.previousOffsetsFiles(camusRunPath))

    val currentTopicsAndOldestTimes = camusReader.topicsAndOldestTimes(currentOffsets).mapValues(currentOffset => currentOffset - delayMilli)
    val previousTopicsAndOldestTimes = if (backfill) {
      log.info(s"Running backfill version, updating folders since the 'beggining of times': $EARLIEST_OF_TIMES")
      currentTopicsAndOldestTimes.mapValues[Long]((currentTime) => EARLIEST_OF_TIMES)
    } else {
      camusReader.topicsAndOldestTimes(previousOffsets).mapValues(previousOffset => previousOffset - delayMilli)
    }

    val finalMap = previousTopicsAndOldestTimes.foldLeft(Map.empty[String, Seq[(Int, Int, Int, Int)]])(
      (map, previousTopicAndTime) => {
        var returnMap = map
        val (previousTopic, previousTime) = previousTopicAndTime
        if ((! previousTopic.matches(topicsBlacklist)) && previousTopic.matches(topicsWhitelist)) {
          if (currentTopicsAndOldestTimes.contains(previousTopic) && (currentTopicsAndOldestTimes.get(previousTopic).get > previousTime)) {
            val hours = finishedHoursInBetween(previousTime, currentTopicsAndOldestTimes.get(previousTopic).get)
            returnMap = map + (previousTopic -> hours)
          } else
            log.info(s"Topic $previousTopic has no new data")
        }
        returnMap
      }
    )
    finalMap
  }

  def flagFullyImportedPartitions(flag: String,
                                  dryRun: Boolean,
                                  topicsAndHours: Map[String, Seq[(Int, Int, Int, Int)]]): Unit = {
    for ((topic, hours) <- topicsAndHours) {
      for ((year, month, day, hour) <- hours) {
        val dir = partitionDirectory(
          props.getProperty(PARTITION_BASE_PATH), topic, year, month, day, hour)
        val partitionPath: Path = new Path(dir)
        if (fs.exists(partitionPath) && fs.isDirectory(partitionPath)) {
          val flagPath = new Path(s"${dir}/${flag}")
          if (! fs.exists(flagPath)) {
            if (! dryRun) {
              fs.create(flagPath)
              log.info(s"Flag created: ${dir}/${flag}")
            } else
              log.info(s"DryRun - Flag would have been created: ${dir}/${flag}")
          }
          else {
            log.warn(s"Flag already exists: ${flagPath.toString}")
          }
        } else
          log.warn(s"Folder does not exist for hour $partitionPath. Can't flag it.")
      }
    }
  }

  case class Params(camusPropertiesFilePath: String = "",
                    datetimeToCheck: Option[String] = None,
                    hadoopCoreSitePath: String = "/etc/hadoop/conf/core-site.xml",
                    hadoopHdfsSitePath: String = "/etc/hadoop/conf/hdfs-site.xml",
                    flag: String = "_IMPORTED",
                    delayHours: Int = 0,
                    dryRun: Boolean = false,
                    backfill: Boolean = false)

  val argsParser = new OptionParser[Params]("Camus Checker") {
    head("Camus partition checker", "")
    note(
      "This job checked for most recent camus run correctness and flag hour partitions when fully imported.")
    help("help") text ("Prints this usage text")

    opt[String]('c', "camus-properties-file") required() valueName ("<path>") action { (x, p) =>
      p.copy(camusPropertiesFilePath = x)
    } text ("Camus configuration properties file path.")

    opt[String]('d', "datetimeToCheck") optional() valueName ("yyyy-mm-dd-HH-MM-SS") action { (x, p) =>
      p.copy(datetimeToCheck = Some(x))
    } text ("Datetime camus run to check (must be present in history folder) - Default to most recent run.")

    opt[String]("hadoop-core-site-file") optional() valueName ("<path>") action { (x, p) =>
      p.copy(hadoopCoreSitePath = x)
    } text ("Hadoop core-site.xml file path for configuration.")

    opt[String]("hadoop-hdfs-site-file") optional() valueName ("<path>") action { (x, p) =>
      p.copy(hadoopHdfsSitePath = x)
    } text ("Hadoop hdfs-site.xml file path for configuration.")

    opt[String]("flag") optional() action { (x, p) =>
      p.copy(flag = x)
    } validate { f =>
      if ((! f.isEmpty) && (f.matches("_[a-zA-Z0-9-_]+"))) success else failure("Incorrect flag file name")
    } text ("Flag file to be used (defaults to '_IMPORTED'.")

    opt[Int]("delay-hours") optional() action { (x, p) =>
      p.copy(delayHours = x)
    } text ("Delay watermarking by X hours prior to current stable offset")

    opt[Unit]("dry-run") optional() action { (_, p) =>
      p.copy(dryRun = true)
    } text ("Only print check result and if flag files would have been created.")

    opt[Unit]("backfill") optional() action { (_, p) =>
      p.copy(backfill = true)
    } text ("Goes through until the 'beggining of times' to flag completed folders.")
  }

  def isLog4JConfigured():Boolean = {
    if (Logger.getRootLogger.getAllAppenders.hasMoreElements)
      return true
    val loggers = LogManager.getCurrentLoggers
    while (loggers.hasMoreElements)
      if (loggers.nextElement.asInstanceOf[Logger].getAllAppenders.hasMoreElements)
        return true
    return false
  }

  def main(args: Array[String]): Unit = {
    if (! isLog4JConfigured)
      org.apache.log4j.BasicConfigurator.configure

    argsParser.parse(args, Params()) match {
      case Some (params) => {
        try {
          log.info("Loading hadoop configuration.")
          val conf: Configuration = new Configuration()
          conf.addResource(new Path(params.hadoopCoreSitePath))
          conf.addResource(new Path(params.hadoopHdfsSitePath))
          fs = FileSystem.get(conf)
          camusReader = new CamusStatusReader(fs)

          log.info("Loading camus properties file.")
          props.load(new FileInputStream(params.camusPropertiesFilePath))

          val camusPathToCheck: Path = {
            val history_folder = props.getProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH)
            if (params.datetimeToCheck.isEmpty) {
              log.info("Getting camus most recent run from history folder.")
              camusReader.mostRecentRun(new Path(history_folder))
            } else {
              val p = new Path(props.getProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH) + "/" + params.datetimeToCheck.get)
              if (fs.isDirectory(p)) {
                log.info("Set job to given datetime to check.")
                p
              } else {
                log.error("The given datetime to check is not a folder in camus history.")
                null
              }
            }
          }
          if (null == camusPathToCheck)
            System.exit(1)

          if (params.delayHours > 0){
            log.warn(s"Watermarking delayed by ${params.delayHours} from stable offsets.")
          }
          log.info("Checking job correctness and computing partitions to flag as imported.")
          val topicsAndHours = getTopicsAndHoursToFlag(
            camusPathToCheck,
            (params.delayHours * 60 * 60 * 1000).toLong, // convert hours to milliseconds
            params.backfill)

          log.info("Job is correct, flag imported partitions.")
          flagFullyImportedPartitions(params.flag, params.dryRun, topicsAndHours)

          log.info("Done.")
        } catch {
          case e: Exception => {
            log.error("An error occurred during execution.", e)
            sys.exit(1)
          }
        }
      }
      case None => {
        log.error("No parameter passed. Please run with --help to see options.")
        sys.exit(1)
      }
    }

  }


}
