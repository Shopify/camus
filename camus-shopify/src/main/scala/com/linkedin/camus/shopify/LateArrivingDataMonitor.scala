package com.linkedin.camus.shopify

import java.io.FileInputStream
import java.util.Properties

import com.linkedin.camus.etl.kafka.reporter.StatsdReporter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import scopt.OptionParser

object LateArrivingDataMonitor {
  val log: Logger = Logger.getLogger(LateArrivingDataMonitor.getClass)

  case class Params(camusPropertiesFilePath: String = "",
                    checkType: String = "window",
                    window: Long = 1000 * 60 * 60 * 6, // 6 hours window
                    numRuns: Int = 1
                   )

  val argsParser = new OptionParser[Params]("com.linkedin.camus.shopify.LateArrivingDataMonitor") {
    head("Camus Late Arriving Data Checker", "")
    note(
      "This Job checks for late arriving data in Camus drops.")
    help("help") text "Prints this usage text"

    opt[String]('c', "camus-properties-file") required() valueName "<path>" action { (x, p) =>
      p.copy(camusPropertiesFilePath = x)
    } text "Camus configuration properties file path."

    opt[String]('t', "check-type") required() action { (x, p) =>
      p.copy(checkType = x)
    } text "Camus check type: \"runs\" or \"window\""

    opt[Long]('w', "window") valueName "<window>" action { (x, p) =>
      p.copy(window = x)
    } text "Hours before last offset to check: only applicable in \"window\" type check"

    opt[Int]('r', "num-runs") optional() valueName "<int>" action { (x, p) =>
      p.copy(numRuns = x)
    } text "Number of camus runs to check: only applicable in \"runs\" type check."

  }


  def checkDropsInWindow(properties: Properties, fs: FileSystem, window: Long): Boolean = {
    log.info(s"Running check on folders dropped in the window of $window milliseconds.")
    log.info("Fetching topics and folders to check...")
    val camusExecutions = new CamusExecutions(properties, fs)
    val foldersToCheck = camusExecutions.droppedFoldersInWindow(window)

    log.info(s"Checking folders to flag violation...")
    checkDrops(properties, fs, foldersToCheck.toList)
  }

  def checkDropsInRuns(properties: Properties, fs: FileSystem, runs: Int): Boolean = {
    log.info(s"Running check on folders dropped in the last $runs Camus runs.")
    val camusExecutions = new CamusExecutions(properties, fs)
    val camusRunPaths = camusExecutions.lastRunDirs(runs)

    log.info(s"Collecting directories touched in $runs last Camus runs: $camusRunPaths")
    val foldersToCheck = camusExecutions.droppedFoldersInRuns(camusRunPaths)

    log.info(s"Checking folders to flag violation...")
    checkDrops(properties, fs, foldersToCheck.toList)
  }

  def checkDrops(properties: Properties, fs: FileSystem, foldersToCheck: List[String]): Boolean = {
    var violationFound = false
    foldersToCheck.foreach(
      folder => {
        log.info(s"Checking $folder")
        val camusDrop = new CamusHourlyDrop(new Path(folder), fs)

        if (camusDrop.hasFlag && camusDrop.isFlagViolated) {
          violationFound = true
          log.error(s"Violation found in ${camusDrop.path}")
          try {
            val violationList = camusDrop.violations
            violationList.foreach({
              fileAndCount =>
                log.error(s"=> late-arriving records in file ${fileAndCount._1}: ${fileAndCount._2}")
            })
            StatsdReporter.gauge(properties, "late-arriving-data", 1L, s"directory:${camusDrop.topicDir}")
          }
          catch {
            case e: Exception => log.error(s"Could not get count of violations for $folder: $e")
          }
        }
      }
    )
    violationFound
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some (params) =>
        if (! (params.checkType == "window" ||  params.checkType == "runs")) {
          log.error(s"ERROR: Unknown check type ${params.checkType}")
          sys.exit(1)
        }
        log.info("Setting up...")
        val fs = FileSystem.get(new Configuration())
        val properties: Properties = new Properties()
        properties.load(new FileInputStream(params.camusPropertiesFilePath))

        val violationFound =
          if (params.checkType == "window") {
           checkDropsInWindow(properties, fs, params.window)
        } else { // runs
          checkDropsInRuns(properties, fs, params.numRuns)
        }
        if (violationFound) {
          log.error("ERROR: One or more folders have late-arriving data.")
          sys.exit(1)
        } else {
          log.info("No late-arriving data found.")
        }
      case None =>
        log.error("missing arguments, use --help")
        sys.exit(1)
    }
  }
}
