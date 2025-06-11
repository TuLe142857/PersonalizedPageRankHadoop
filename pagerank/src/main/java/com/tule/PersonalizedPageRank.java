package com.tule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;

public class PersonalizedPageRank {

    public static boolean run(String inPath, String outPath, double dampingFactor, int maxIter)
            throws Exception
    {
//        System.out.println("Từ từ, chưa code kịp :(");
        Configuration conf = new Configuration();

        // set total source node
        long totalUniqueSourceNode = GraphUtils.countUniqueSourceNodes(conf, inPath, outPath + "/tempDirForCount");
        conf.setLong(Constants.TOTAL_SOURCE_NODE_CONFIG, totalUniqueSourceNode);

        // set dumping factor
        conf.setDouble(Constants.DAMPING_FACTOR_CONFIG, dampingFactor);



        //run loop
        String initGraphPathOut = outPath+"/normalizeGraph";
        if (!initGraph(conf, inPath, initGraphPathOut))
            return false;

        String input = initGraphPathOut;
        String output = outPath + "/loop_" + String.valueOf(0);
        for(int i = 0; i < maxIter; i++){
            if (!loopMapReduce(conf, input, output)){
                return false;
            }
            input = output;
            output = outPath + "/loop_" + String.valueOf(i+1);
        }
        return true;
    }

    public static boolean initGraph(Configuration conf, String inPath, String outPath)
            throws Exception
    {
        Job job = Job.getInstance(conf, "job initialize graph(personalized pagerank)");

        //class
        job.setJarByClass(PersonalizedPageRank.class);
        job.setMapperClass(PersonalizedPageRank.PPRInitMapper.class);
        job.setReducerClass(PersonalizedPageRank.PPRInitReducer.class);


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

    public static boolean loopMapReduce(Configuration conf, String inPath, String outPath)
            throws Exception
    {
        Job job = Job.getInstance(conf, "job loop(personalized pagerank)");

        // class
        job.setJarByClass(PersonalizedPageRank.class);
        job.setMapperClass(PersonalizedPageRank.PPRMapper.class);
        job.setReducerClass(PersonalizedPageRank.PPRReducer.class);

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

    /*---------------------------------------------------------
                    INITIALIZE GRAPH
    ------------------------------------------------------------*/

    /**
     * throw key, value:
     *  - from node to node
     *  - source node, SOURCE_NODE_SYMBOL
     */
    public static class PPRInitMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text textV = new Text();
        private Text textK = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.charAt(0) == '#'){
                return;
            }
            String []parts = line.split(Constants.SEPARATOR);

            // source node
            if (parts.length == 1){
                //counter += 1
                textK.set(parts[0]);
                textV.set(Constants.SOURCE_NODE_SYMBOL);
                context.write(textK, textV);
            }
            else if (parts.length == 2){
                textK.set(parts[0]);
                textV.set(parts[1]);
                context.write(textK, textV);
            }
        }
    }

    /**
     * throw key, value:
     *  - Node , [PPR] SEP [v] [list of outlinks separated SEP]
     *  - v: probability of teleport(if normal node = 0, if source node > 0)
     *  - SEP: Constants.SEPARATOR
     */
    public static class PPRInitReducer extends Reducer<Text, Text, Text, Text>{
        private Text textV = new Text();
        private Text textK = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            long sourceNodeCount = context.getConfiguration().getLong(Constants.TOTAL_SOURCE_NODE_CONFIG, 1L);

            double pageRank = 0.0;

            // xac suat teleport, source page: >0 ; normal: 0
            double prob = 0.0;

            StringBuilder outlinks = new StringBuilder();

            for (Text v : values){
                String line = v.toString();
                String []parts = line.split(Constants.SEPARATOR);

                // source node
                if(parts[0].equals(Constants.SOURCE_NODE_SYMBOL)){
                    prob = 1.0/sourceNodeCount;
                    continue;
                }
                outlinks.append(parts[0]);
                outlinks.append(Constants.SEPARATOR);
            }


            pageRank = prob;
            // PR V
            outlinks.insert(0, String.valueOf(pageRank) + Constants.SEPARATOR + String.valueOf(prob) + Constants.SEPARATOR);

            textV.set(outlinks.toString());
            context.write(key, textV);

        }
    }


    /*---------------------------------------------------------
                MAPREDUCE FOR PPR
    ------------------------------------------------------------*/


    public static class PPRMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text textV = new Text();
        private Text textK = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line  = value.toString();
            String []parts = line.split(Constants.SEPARATOR);

            double ppr = Double.parseDouble(parts[1]);
            int numOfNeighbor = parts.length-3;
            double sharePPR = ppr/numOfNeighbor;
            textV.set(String.valueOf(sharePPR));

            // share PPR
            for (int i = 3; i < parts.length; i++){
                textK.set(parts[i]);
                context.write(textK, textV);
            }

            // phát cấu trúc graph
            textK.set(parts[0]);
            int offset = parts[0].length() + parts[1].length() +  Constants.SEPARATOR.length()*2;
            textV.set(Constants.LINK_SEPARATOR + line.substring(offset));
            context.write(textK, textV);
        }
    }

    public static class PPRReducer extends Reducer<Text, Text, Text, Text>{
        private Text textV = new Text();
        private Text textK = new Text();


        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // test mapp
            double teleportProb = 0.0;
            double sumSharedPPR = 0.0;
            double damping_factor = context.getConfiguration().getDouble(Constants.DAMPING_FACTOR_CONFIG, 0.85);
            String graphStruct = "";
            for (Text v: values){
                String line = v.toString();
                // graph struct
                if (line.startsWith(Constants.LINK_SEPARATOR)){
                    graphStruct = line.substring(Constants.LINK_SEPARATOR.length());
                    teleportProb = Double.parseDouble(graphStruct.split(Constants.SEPARATOR)[0]);
                }
                // share PPR
                else {
                    sumSharedPPR += Double.parseDouble(line.substring(Constants.LINK_SEPARATOR.length()));
                }
            }

            double newPPR = ((1.0-damping_factor)*teleportProb) + sumSharedPPR*damping_factor;
            textV.set(String.valueOf(newPPR) + Constants.SEPARATOR + graphStruct);
            context.write(key, textV);
        }
    }
}
