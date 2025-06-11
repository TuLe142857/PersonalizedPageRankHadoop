package com.tule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class GraphUtils {

    public static class CountNodeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static Text EMPTY_TEXT = new Text("");
        private Text nodeText = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("#") || line.trim().isEmpty()) {
                return;
            }
            String[] parts = line.split(Constants.SEPARATOR);

            if (parts.length < 2)
                return;
            nodeText.set(parts[0]);
            context.write(nodeText, EMPTY_TEXT);

            nodeText.set(parts[1]);
            context.write(nodeText, EMPTY_TEXT);
        }
    }

    public static class CountNodeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.getCounter(Constants.Counters.UNIQUE_NODE_COUNTER).increment(1);
        }
    }

    public static long countUniqueNodes(Configuration conf, String inGraphPath, String tempOutPath)
            throws Exception
    {
        Job job = Job.getInstance(conf, "Count Unique Nodes");
        job.setJarByClass(GraphUtils.class);

        job.setMapperClass(CountNodeMapper.class);
        job.setReducerClass(CountNodeReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path jobTempOutputPath = new Path(tempOutPath);
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(jobTempOutputPath)) {
            fs.delete(jobTempOutputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(inGraphPath));
        FileOutputFormat.setOutputPath(job, jobTempOutputPath);

        boolean jobSuccess = job.waitForCompletion(true);

        if (!jobSuccess) {
            throw new RuntimeException(":) Some thing wrong!");
        }

        return job.getCounters()
                .findCounter(Constants.Counters.UNIQUE_NODE_COUNTER)
                .getValue();
    }


    public static class CountSourceNodeMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text textV = new Text("");
        private Text textK = new Text("");

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String []parts = line.split(Constants.SEPARATOR);
            if (parts.length == 1){
                textK.set(parts[0]);
                context.write(textK, textV);
            }
        }
    }

    public static class CountSourceNodeReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            context.getCounter(Constants.Counters.UNIQUE_SOURCE_NODE_COUNTER).increment(1L);
        }
    }

    public static long countUniqueSourceNodes(Configuration conf, String inGraphPath, String tempOutPath)
            throws Exception
    {
        Job job = Job.getInstance(conf, "Count Unique Nodes");
        job.setJarByClass(GraphUtils.class);

        job.setMapperClass(CountSourceNodeMapper.class);
        job.setReducerClass(CountSourceNodeReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path jobTempOutputPath = new Path(tempOutPath);
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(jobTempOutputPath)) {
            fs.delete(jobTempOutputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(inGraphPath));
        FileOutputFormat.setOutputPath(job, jobTempOutputPath);

        boolean jobSuccess = job.waitForCompletion(true);

        if (!jobSuccess) {
            throw new RuntimeException(":) Some thing wrong!");
        }

        return job.getCounters()
                .findCounter(Constants.Counters.UNIQUE_SOURCE_NODE_COUNTER)
                .getValue();
    }
}
