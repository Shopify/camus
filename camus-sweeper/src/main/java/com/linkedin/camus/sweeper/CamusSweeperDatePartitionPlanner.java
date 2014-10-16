package com.linkedin.camus.sweeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.sweeper.utils.DateUtils;

public class CamusSweeperDatePartitionPlanner extends CamusSweeperPlanner
{
  private static final Logger _log = Logger.getLogger(CamusSweeperDatePartitionPlanner.class);

  private DateTimeFormatter dayFormatter;
  private DateUtils dUtils;

  @Override
  public CamusSweeperPlanner setPropertiesLogger(Properties props, Logger log)
  {
    dUtils = new DateUtils(props);
    dayFormatter = dUtils.getDateTimeFormatter("YYYY/MM/dd/HH");
    return super.setPropertiesLogger(props, log);
  }

  @Override
  public List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs) throws IOException
  {
    int daysAgo = Integer.parseInt(props.getProperty("days.ago", "0"));
    int numDays = Integer.parseInt(props.getProperty("num.days", "5"));

    DateTime midnight = dUtils.getMidnight();
    DateTime startDate = midnight.minusDays(daysAgo);

    System.out.println("star date: " + startDate.toString());

    List<Properties> jobPropsList = new ArrayList<Properties>();
    for (int i = 0; i < numDays; ++i)
    {

      for(int j = 0; j < 24; ++j){

        if (dUtils.getCurrentTime().getHourOfDay() == j ){
          System.out.println("Skipping hour: " + String.valueOf(j));
          continue;
        }

        Properties jobProps = new Properties(props);
        jobProps.putAll(props);

        jobProps.put("topic", topic);

        DateTime currentDate = startDate.minusDays(i).plusHours(j);

        String directory = dayFormatter.print(currentDate);
        Path sourcePath = new Path(inputDir, directory);
        if (!fs.exists(sourcePath))
        {
          System.out.println("Source path: " + sourcePath + " does not exist.");
          continue;
        }

        List<Path> sourcePaths = new ArrayList<Path>();
        sourcePaths.add(sourcePath);

        String[] parts = directory.split("/");
        String output = "year=" + parts[0] + "/";
        output += "month=" + parts[1] + "/";
        output += "day=" + parts[2] + "/";
        output += "hour=" + parts[3] + "/";

        Path destPath = new Path(outputDir, output);

        jobProps.put("input.paths", sourcePath.toString());
        jobProps.put("dest.path", destPath.toString());

        System.out.println("DEST PATH: " + destPath.toString() + " EXISTS: " + String.valueOf(fs.exists(destPath)));

        if (!fs.exists(destPath))
        {
          System.out.println(topic + " dest dir " + directory + " doesn't exist or . Processing.");
          jobPropsList.add(jobProps);
        }
        else if (shouldReprocess(fs, sourcePaths.get(0), destPath))
        {
          System.out.println(topic + " dest dir " + directory + " has a modified time before the source. Reprocessing.");
          jobProps.put("input.paths", sourcePath.toString() + "," + destPath.toString());
          jobPropsList.add(jobProps);
        }
        else
        {
          System.out.println(topic + " skipping " + directory);
        }
      }
    }

    return jobPropsList;

  }



}
