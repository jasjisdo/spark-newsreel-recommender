package de.dailab.newsreel.recommender.common.util;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;

import static de.dailab.newsreel.recommender.common.util.ConfigLoader.load;

/**
 * Created by domann on 07.04.16.
 */
public class SharedStreamContext {

    private static final Logger log = Logger.getLogger(SharedStreamContext.class);

    private static SharedStreamContext instance;

    private SparkConf sparkConf;
    private JavaStreamingContext jssc;

    public SharedStreamContext() {
        ConfigLoader.Conf conf = null;
        try {
            conf = load();
        } catch (IOException ioe) {
            log.error("failed to load configuration file, using defaults.", ioe);
        }
        sparkConf = new SparkConf()
                .setAppName(conf.getAppName())
                .setMaster(conf.getMaster())
                .set("spark.executor.memory", conf.getMaxMem())
                .set("spark.driver.memory", conf.getDriverMem());

        log.info(sparkConf.toString());
        log.info("init spark...");
        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    }

    /**
     * This method may only be used private.
     * Since security does not demand otherwise, it is
     * defined public non the less.
     *
     * @return The instance of this singleton that provides direct access
     * to the spark context.
     */
    public static SharedStreamContext getInstance() {
        if (instance == null) {
            instance = new SharedStreamContext();
        }
        return instance;
    }



}
