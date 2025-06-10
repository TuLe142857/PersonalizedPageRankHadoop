package com.tule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

public class Main {
    private static Double DAMPING_FACTOR = 0.85;
    private static int MAX_ITERATIONS = 2;
    private static String IN_PATH = "";
    private static String OUT_PATH = "";

    // PR or PPR
    private static boolean IS_BASIC_PAGERANK = true;

    public static void main(String []args)throws Exception{
        ArgsParser.parse(args);

        System.out.println("Program Start");
        System.out.println("PageRank\n");
        System.out.printf("Input: %s\n", IN_PATH);
        System.out.printf("Output: %s\n", OUT_PATH);
        System.out.printf("DampingFactor: %f\n", DAMPING_FACTOR);
        System.out.printf("MaxIteration: %d\n", MAX_ITERATIONS);
        System.out.printf("Type of Algorithm: %s\n", (IS_BASIC_PAGERANK)?("PageRank"):("PersonalizedPageRank"));

        if (IS_BASIC_PAGERANK)
            PageRank.run(IN_PATH, OUT_PATH, DAMPING_FACTOR, MAX_ITERATIONS);
        else
            PersonalizedPageRank.run(IN_PATH, OUT_PATH, DAMPING_FACTOR, MAX_ITERATIONS);
    }

    private static class ArgsParser{

        // command keyword
        private static final String KEY_DAMPING = "--damping";
        private static final String KEY_DAMPING_ALIAS = "-d";

        private static final String KEY_LIMIT_LOOP = "--limit";
        private static final String KEY_LIMIT_LOOP_ALIAS = "-l";

        private static final String KEY_INPUT = "--input";
        private static final String KEY_INPUT_ALIAS = "-i";

        private static final String KEY_OUTPUT = "--output";
        private static final String KEY_OUTPUT_ALIAS = "-o";

        private static final String KEY_HELP = "--help";
        private static final String KEY_HELP_ALIAS = "-h";

        public static void parse(String []args){
            System.out.println("Arguments:");
            for (String a:args)
                System.out.println(a);
            if (args.length > 2){
                for (int i = 0; i < args.length; i+=2){
                    String key = args[i];
                    String value = args[i+1];

                    if (key.equals(KEY_DAMPING) || key.equals(KEY_DAMPING_ALIAS))
                        DAMPING_FACTOR = Math.max(Double.parseDouble(value), 1.0);
                    else if (key.equals(KEY_LIMIT_LOOP) || key.equals(KEY_LIMIT_LOOP_ALIAS))

                        MAX_ITERATIONS = Math.max(Integer.parseInt(value), 1);
                    else if (key.equals(KEY_INPUT) || key.equals(KEY_INPUT_ALIAS))
                        IN_PATH = value.trim();
                    else if (key.equals(KEY_OUTPUT) || key.equals(KEY_OUTPUT_ALIAS))
                        OUT_PATH = value.trim();
                    else if (key.equals(KEY_HELP) || key.equals(KEY_HELP_ALIAS))
                        printHelp(null);
                    else
                        printHelp("Invalid command options");

                }
            }
            if (IN_PATH.isEmpty() || OUT_PATH.length() == 0){
                printHelp("No input/output path provided!");
            }
        }

        public static void printHelp(String err) {
            if (err != null)
                System.err.println("ERROR: " + err + ".\n");

            System.out.println("pagerank-1.0-SNAPSHOT-jar-with-dependencies.jar com.tule.Main " + KEY_INPUT + " <input> " + KEY_OUTPUT + " <output>\n");
            System.out.println("Options:\n");
            System.out.println("    " + KEY_INPUT + "    (" + KEY_INPUT_ALIAS + ")    <input>       The directory of the input graph [REQUIRED]");
            System.out.println("    " + KEY_OUTPUT + "   (" + KEY_OUTPUT_ALIAS + ")    <output>      The directory of the output result [REQUIRED]");
            System.out.println("    " + KEY_DAMPING + "  (" + KEY_DAMPING_ALIAS + ")    <damping>     The damping factor [OPTIONAL]");
            System.out.println("    " + KEY_LIMIT_LOOP + "    (" + KEY_LIMIT_LOOP_ALIAS + ")    <iterations>  The amount of iterations [OPTIONAL]");
            System.out.println("    " + KEY_HELP + "     (" + KEY_HELP_ALIAS + ")                  Display the help text\n");
            System.exit(0);
        }
    }
}
