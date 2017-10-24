package com.linkedin.camus.etl.kafka.reporter;

import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import java.util.Map;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.util.ToolRunner;

import com.linkedin.camus.etl.kafka.reporter.TimeReporter;


public class StatsdReporter extends TimeReporter {

  public static final String STATSD_ENABLED = "statsd.enabled";
  public static final String STATSD_HOST = "statsd.host";
  public static final String STATSD_PORT = "statsd.port";

  private static boolean statsdEnabled;
  private static StatsDClient statsd;

  public void report(Job job, Map<String, Long> timingMap) throws IOException {
    super.report(job, timingMap);
    submitCountersToStatsd(job);
  }

  private static StatsDClient getClient(Configuration conf) {
    return new NonBlockingStatsDClient("Camus", getStatsdHost(conf), getStatsdPort(conf),
            new String[] { "camus:counters" });
  }

  private void submitCountersToStatsd(Job job) throws IOException {
    Counters counters = job.getCounters();
    Configuration conf = job.getConfiguration();
    if (getStatsdEnabled(conf)) {
      StatsDClient statsd = getClient(conf);
      for (CounterGroup counterGroup : counters) {
        for (Counter counter : counterGroup) {
          statsd.gauge(counterGroup.getDisplayName() + "." + counter.getDisplayName(), counter.getValue());
        }
      }
    }
  }

  public static Boolean getStatsdEnabled(Configuration conf) {
    return conf.getBoolean(STATSD_ENABLED, false);
  }

  public static String getStatsdHost(Configuration conf) {
    return conf.get(STATSD_HOST, "localhost");
  }

  public static int getStatsdPort(Configuration conf) {
    return conf.getInt(STATSD_PORT, 8125);
  }

  public static void gauge(Configuration conf, String metric, Long value, String... tags) {
    if (conf.getBoolean(STATSD_ENABLED, false)) {
      StatsDClient statsd = getClient(conf);
      statsd.gauge(metric, value, tags);
    }
  }
}
