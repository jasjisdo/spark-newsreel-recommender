package de.dailab.newsreel.recommender.common.util;

import static de.dailab.newsreel.recommender.common.util.ConfigLoader.*;

import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Created by domann on 10.12.15.
 *
 * Singelton that manages the spark context related methods
 */
public class SharedService {
    /**
     * Define the default log
     */
    private final static Logger log = LoggerFactory.getLogger(SharedService.class);


    private static SharedService instance;

    private SparkConf sparkConf;
    private JavaSparkContext sc;

    /**
     * Initialize spark configuration and context only once.
     * This architecture makes sure, that all classes are loaded
     * before spark context is initialized.
     */
    private SharedService() {
        ConfigLoader.Conf conf = null;
        try {
            conf = load();
        } catch (IOException e) {
            log.error("failed to load configuration file, using defaults.", e);
        }
        sparkConf = new SparkConf()
                .setAppName(conf.getAppName())
                .setMaster(conf.getMaster())
                .set("spark.executor.memory", conf.getExeMem())
                .set("spark.executor.cores", conf.getExeCores())
                //.set("spark.driver.memory", conf.getDriverMem())
                .set("spark.eventLog.enabled", conf.getEventlogging())
                .set("spark.eventLog.dir", conf.getEventlogDir())
                .set("spark.local.ip", conf.getLocalIp())
                .set("spark.scheduler.mode", "FAIR")
                .set("spark.fileserver.port", conf.getFileServerPort())
                .set("spark.fileserver.uri", conf.getFileServerURI())
                //.set("spark.driver.extraLibraryPath", conf.getJarFolder())
                //.set("spark.executor.extraLibraryPath", conf.getJarFolder())
                .set( "spark.driver.port", conf.getDriverPort())
                .set( "spark.driver.host", conf.getDriverHost());

        log.info("driver host: " + conf.getDriverHost());

        log.info("context started");
        try {
            File folder = new File(conf.getJarFolder());
            log.info("searching for jars in "+folder.getAbsolutePath());
            File[] files = folder.listFiles(file -> file.isFile() && file.getAbsolutePath().endsWith(".jar"));
            String[] jars = new String[files.length];
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            builder.append(files[0]);
            String absPath = files[0].getAbsolutePath();
            jars[0] = absPath;
            for (int i=1; i < files.length; i++) {
                if (files[i].isFile()) {
                    String absolutePath = files[i].getAbsolutePath();
                    builder.append(", ");
                    builder.append(absolutePath);
                    jars[i] = absolutePath;
                }
            }
            builder.append("]");
            log.debug("adding jars: " + builder.toString());
            sparkConf.setJars(jars);
        } catch (Exception e) {
            log.error("failed to load additional jars", e);
        }

        log.info(sparkConf.toString());
        log.info("init spark...");
        sc = new JavaSparkContext(sparkConf);
    }

    /**
     * This method may only be used private.
     * Since security does not demand otherwise, it is
     * defined public non the less.
     *
     * @return The instance of this singleton that provides direct access
     * to the spark context.
     */
    public static SharedService getInstance() {
        if (instance == null) {
            instance = new SharedService();
        }
        return instance;
    }

    public static JavaSparkContext getContext() {
        return getInstance().sc;
    }

    /**
     * parallelize a list of objects using spark.
     * @param objects that should be parallelized
     * @param <T> type of objects
     * @return a JavaRDD of type T wrapping the objects
     */
    public static <T> JavaRDD<T> parallelize(List<T> objects) {
        instance = getInstance();
        return instance.sc.parallelize(objects);
    }

    /**
     * parallelize an array of objects using spark.
     * @param objects that should be parallelized
     * @param <T> type of objects
     * @return a JavaRDD of type T wrapping the objects
     */
    public static <T> JavaRDD<T> parallelize(T... objects) {
        return parallelize(Arrays.asList(objects));
    }

    /**
     * parallelize a list of tuples using spark.
     * @param tuples that should be parallelized
     * @param <K,V> type of key and value objects
     * @return a JavaPairRDD of key type K and value type V wrapping the tuple
     */
    public static <K,V> JavaPairRDD<K,V> parallelizePairs(List<Tuple2<K,V>> tuples) {
        instance = getInstance();
        return instance.sc.parallelizePairs(tuples);
    }

    /**
     * parallelize a map to tuples using spark.
     * @param map that should be parallelized
     * @param  map with <K,V> type of key and value objects
     * @return a JavaPairRDD of key type K and value type V wrapping the tuple
     */
    public static <K,V> JavaPairRDD<K,V> parallelizePairs(Map<? extends K, ? extends V> map) {
        instance = getInstance();
        List<Tuple2<K,V>> input = new ArrayList<>();
        for(K key : map.keySet()) {
            input.add(new Tuple2<K, V>(key, map.get(key)));
        }
        return instance.sc.parallelizePairs(input);
    }

    /**
     *
     * @param tuple
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> JavaPairRDD<K,V> parallelizePairs(Tuple2<K, V>... tuple) {
        return parallelizePairs(Arrays.asList(tuple));
    }

}
