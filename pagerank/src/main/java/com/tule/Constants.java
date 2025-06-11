package com.tule;

public class Constants {
    public static enum Counters{
        UNIQUE_NODE_COUNTER,
        UNIQUE_SOURCE_NODE_COUNTER
    }

    //Config.set<...>()
    public static String TOTAL_NODE_CONFIG = "TOTAL_NODE_CONFIGURATION";
    public static String TOTAL_SOURCE_NODE_CONFIG = "TOTAL_SOURCE_NODE_CONFIGURATION";
    public static String DAMPING_FACTOR_CONFIG = "DAMPING_FACTOR_CONFIGURATION";

    public static String SOURCE_NODE_SYMBOL = "|SOURCE_NODE";

    // text separator
    public static String SEPARATOR = "\t";
    public static String LINK_SEPARATOR = "|";
}
