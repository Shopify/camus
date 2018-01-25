package com.linkedin.camus.shopify

import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat
import org.apache.hadoop.fs.{FileSystem, Path}

class CamusHourlyDrop(folderPath: Path, fileSystem: FileSystem) {
  val path = folderPath
  val fs = fileSystem
  val FLAG_NAME = "_IMPORTED"

  def flagPath: Path = new Path(path.toString + s"/$FLAG_NAME")

  def hasFlag: Boolean = fs.exists(flagPath)

  def flagWrittenAt: Long = fs.getFileStatus(flagPath).getModificationTime

  def lastFileWrittenAt: Long = {
    fs.listStatus(path).
      filter(status => status.getPath.getName != FLAG_NAME).
      map(status => status.getModificationTime).
      max
  }

  def topicDir: String = {
    new Path(path.toString.replace(EtlMultiOutputFormat.ETL_DESTINATION_PATH, ""))
      .getParent.getParent.getParent.getParent.getName
  }

  def isFlagViolated: Boolean = lastFileWrittenAt > flagWrittenAt

  def getCount(camusFileName: String): Long = {
    // This relies on how we format file names in EtlMultiOutputCommitter.getPartitionedPath
    // File names have the following pattern: TopicName.BrokerId.PartitionId.NumberRecords.FinalOffset.UTC.gz
    camusFileName.split("\\.").takeRight(4).head.toLong
  }

  def violationCount: Long = {
    val flagTime = flagWrittenAt
    fs.listStatus(path).
      filter(status => status.getPath.getName != FLAG_NAME && status.getModificationTime > flagTime).
      map(status => status.getPath.getName).
      map(name => getCount(name)).
      sum
  }

  def violations: List[(String, Long)] = {
    val flagTime = flagWrittenAt
    fs.listStatus(path).
      filter(status => status.getPath.getName != FLAG_NAME && status.getModificationTime > flagTime).
      map(status => status.getPath).
      map(path => (path.toString, getCount(path.getName))).toList
  }
}
