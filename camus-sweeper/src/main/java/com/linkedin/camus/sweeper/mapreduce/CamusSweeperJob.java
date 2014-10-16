package com.linkedin.camus.sweeper.mapreduce;

import com.linkedin.camus.sweeper.utils.RelaxedAvroSerialization;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public abstract class CamusSweeperJob
{
  protected Logger log;

  public CamusSweeperJob setLogger(Logger log)
  {
    this.log = log;
    return this;
  }
  public abstract void configureJob(String topic, Job job);

  protected void configureInput(Job job,
                                Class<? extends InputFormat> inputFormat,
                                Class<? extends Mapper> mapper,
                                Class<?> mapOutKey,
                                Class<?> mapOutValue)
  {
    job.setInputFormatClass(inputFormat);
    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(mapOutKey);
    job.setMapOutputValueClass(mapOutValue);
    RelaxedAvroSerialization.addToConfiguration(job.getConfiguration());
  }

  protected void configureOutput(Job job,
                                 Class<? extends OutputFormat> outputFormat,
                                 Class<? extends Reducer> reducer,
                                 Class<?> outKey,
                                 Class<?> outValue)
  {
    job.setOutputFormatClass(outputFormat);
    job.setReducerClass(reducer);
    job.setOutputKeyClass(outKey);
    job.setOutputValueClass(outValue);
  }

    protected void configureOutput(Job job,
                                   Class<? extends OutputFormat> outputFormat,
                                   Class<? extends Reducer> reducer,
                                   Class<?> outKey,
                                   Class<?> outValue,
                                   Class<? extends RawComparator> comparator)
    {
        job.setOutputFormatClass(outputFormat);
        job.setReducerClass(reducer);
        job.setOutputKeyClass(outKey);
        job.setOutputValueClass(outValue);
        job.setSortComparatorClass(comparator);
    }

  protected String getConfValue(Job job, String topic, String key, String defaultStr){
    return job.getConfiguration().get(topic + "." + key, job.getConfiguration().get(key, defaultStr));
  }

  protected String getConfValue(Job job, String topic, String key){
    return getConfValue(job, topic, key, null);
  }
}
