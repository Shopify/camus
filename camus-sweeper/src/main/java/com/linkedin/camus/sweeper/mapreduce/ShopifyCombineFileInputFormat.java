package com.linkedin.camus.sweeper.mapreduce;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class ShopifyCombineFileInputFormat extends CombineFileInputFormat<LongWritable, Text> {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit) split, context, ShopifyCombineRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    public static class ShopifyCombineRecordReader extends RecordReader<LongWritable, Text> {
        private long startOffset;
        private long end;
        private long pos;
        private FileSystem fs;
        private Path path;
        private LongWritable key;
        private Text value;

        private FSDataInputStream fileIn;
        private LineReader reader;

        public ShopifyCombineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
            this.path = split.getPath(index);
            fs = this.path.getFileSystem(context.getConfiguration());
            this.startOffset = split.getOffset(index);
            this.end = startOffset + split.getLength(index);

            fileIn = fs.open(path);
            reader = new LineReader(fileIn);
            this.pos = startOffset;
        }

        @Override
        public void initialize(InputSplit arg0, TaskAttemptContext arg1)
                throws IOException, InterruptedException {
            // Won't be called, use custom Constructor
            // `CFRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index)`
            // instead
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public float getProgress() throws IOException {
            if (startOffset == end) {
                return 0;
            }
            return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (key == null) {
                key = new LongWritable();
            }
            key.set(pos);
            if (value == null) {
                value = new Text();
            }
            value.clear();

            int newSize = 0;
            if (pos < end) {
                newSize = reader.readLine(value);
                pos += newSize;
            }
            if (newSize == 0) {
                key = null;
                value = null;
                return false;
            } else {
                return true;
            }
        }
    }
}
