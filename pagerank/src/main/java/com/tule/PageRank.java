package com.tule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;

public class PageRank {
    public static boolean run(String inPath, String outPath, double dampingFactor, int maxIter)throws Exception{
        Configuration conf = new Configuration();
        long totalNodes = GraphUtils.countUniqueNodes(conf, inPath, outPath+"/tempDirForCount");
        System.out.println("There's " + totalNodes + " nodes");
        conf.setDouble(Constants.DAMPING_FACTOR_CONFIG, dampingFactor);
        conf.setLong(Constants.TOTAL_NODE_CONFIG, totalNodes);
        //config TotalNode
        if (!initGraph(conf, inPath, outPath+"/normalizeGraph")){
            return false;
        }

        String input = outPath+"/normalizeGraph";
        String output = outPath + "/loop_" + String.valueOf(0);
        //Config damping factor
        for(int i = 0; i < maxIter; i++){
            if (!loopMapReduce(conf, input, output)){
                return false;
            }
            input = output;
            output = outPath + "/loop_" + String.valueOf(i+1);
        }
        return true;
    }

    public static boolean initGraph(Configuration conf, String inPath, String outPath) throws Exception{
        Job job = Job.getInstance(conf, "job initialize graph");

        //class
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRInitMapper.class);
        job.setReducerClass(PRInitReducer.class);


        // data type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //file io
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(inPath);
        Path outputPath = new Path(outPath);
        if (fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }

    public static boolean loopMapReduce(Configuration conf, String inPath, String outPath) throws Exception{
        Job job = Job.getInstance(conf, "job loop");

        // class
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRMapper.class);
        job.setReducerClass(PRReducer.class);

        //data type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //file io
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(inPath);
        Path outputPath = new Path(outPath);
        if (fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // run job
        return job.waitForCompletion(true);
    }

    /*----------------------------
            INITIALIZE GRAPH, need config total node
    ------------------------------*/

    /**
     *  return node, node
     */
    public static class PRInitMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text textK = new Text();
        private Text textV = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.charAt(0) == '#'){
                return;
            }

            String []parts = line.split(Constants.SEPARATOR);
            if (parts.length == 2){
                textK.set(parts[0]);
                textV.set(parts[1]);
                context.write(textK, textV);

            }
        }
    }

    /**
     * Return Node  [PR] [outlink separate by tab]
     */
    public static class PRInitReducer extends Reducer<Text, Text, Text, Text>{
        private Text textK = new Text();
        private Text textV = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuilder line = new StringBuilder();
            long totalNode = context.getConfiguration().getLong(Constants.TOTAL_NODE_CONFIG, 1L);
            Double defaultPR = 1.0/totalNode;
            line.append(defaultPR);
            line.append(Constants.SEPARATOR);
            for (Text v : values){
                line.append(v.toString());
                line.append(Constants.SEPARATOR);
            }
            context.write(key, new Text(line.toString()));
        }
    }


    /*----------------------------
        MAPREDUCE FOR PAGERANK, need config dumping factor
    ------------------------------*/

    /**
     * return (node, pr)
     * (node |outlinks separate by tab)
     */
    public static class PRMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text textK = new Text();
        private Text textV = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String []parts = line.split(Constants.SEPARATOR);
            System.out.println(parts);
            System.out.println("--------------------------------------------------------");
            if (parts.length < 2)
                return;

            String page = parts[0];
            String pageRank = parts[1];

            // phát sharePageRank tới node khác
            if (parts.length > 2){
                double sharePageRank = Double.parseDouble(pageRank)/(parts.length - 2);

                // phát tới các node khác
                for (int i = 2; i < parts.length; i++){
                    textK.set(parts[i]);
                    textV.set(Double.toString(sharePageRank));
                    context.write(textK, textV);
                }
            }

            // phát lại cấu trúc đồ thị
            String outLinks = line.substring(parts[0].length() +parts[1].length() + Constants.SEPARATOR.length()*2);
            textK.set(page);
            textV.set(Constants.LINK_SEPERATOR + outLinks);
            context.write(textK, textV);

        }
    }

    public static class PRReducer extends Reducer<Text, Text, Text, Text>{
        private Text textK = new Text();
        private Text textV = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String outLinks = "";
            double dampingFactor = context.getConfiguration().getDouble(Constants.DAMPING_FACTOR_CONFIG, 0.85);
            double newPageRank = (1 - dampingFactor);
            for(Text v: values){
                String line = v.toString();
                // Nếu là cấu trúc đồ thị (có link sep)
                if (line.startsWith(Constants.LINK_SEPERATOR)){
                    outLinks = line.substring(Constants.SEPARATOR.length());
                }
                // Nếu là sharePageRank
                else {
                    double sharePageRank = Double.parseDouble(line);
                    newPageRank += sharePageRank*dampingFactor;
                }

            } //end for
            context.write(key, new Text(Double.toString(newPageRank) + Constants.SEPARATOR + outLinks));

        }
    }
}
