package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;


public class ShopifyPartitioner extends Partitioner {

    protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH";
    protected DateTimeFormatter outputDateFormatter = null;

    @Override
    public String encodePartition(JobContext context, IEtlKey etlKey) {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        return String.valueOf(DateUtils.getPartition(outfilePartitionMs, etlKey.getTime(), outputDateFormatter.getZone()));
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic, String brokerId, int partitionId, String encodedPartition) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic).append("/");
        if (context.getConfiguration().get(EtlMultiOutputFormat.ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY) != "") {
            sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append("/");
        }
        DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
        sb.append(bucket.toString(outputDateFormatter));
        return sb.toString();
    }

    @Override
    public void setConf(Configuration conf)
    {
        if (conf != null){
            outputDateFormatter = DateUtils.getDateTimeFormatter(OUTPUT_DATE_FORMAT, DateTimeZone.forID(conf.get(EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE, "America/Los_Angeles")));
        }

        super.setConf(conf);
    }
}
