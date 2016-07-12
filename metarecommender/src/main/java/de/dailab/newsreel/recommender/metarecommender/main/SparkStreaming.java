package de.dailab.newsreel.recommender.metarecommender.main;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by domann on 04.04.16.
 */
public class SparkStreaming {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        //jssc.receiverStream();
        //jssc.actorStream()

    }


}
