package de.dailab.newsreel.recommender.metarecommender.main;

import de.dailab.newsreel.recommender.common.item.Item;
import de.dailab.newsreel.recommender.common.recommender.Recommender;
import de.dailab.newsreel.recommender.metarecommender.voidfunc.JettyFallbackStartFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by domann on 07.12.15.
 */
@Deprecated
public class FallbackMain {
    public static void main(String[] args) {

        /*
         * This RDD represents the post send data from plista webclient input data.
         */
        Map<Recommender, JavaRDD<? extends Item> > inputMap = null;

        /*
         * This RDD represents result data received from the recommenders.
         */
        Map< Recommender, JavaRDD<? extends Item> > resultMap = null;

        /*
         * Initialize spark context which representing the spark app/job.
         */
        SparkConf conf = new SparkConf()
                .setAppName("Plista Recomendation Application / Job")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
         * Set embedded jetty port to 8088
         */
        List<Integer> port = Arrays.asList(8088);

        /*
         * Use custom void function to start jetty sever.
         */
        JettyFallbackStartFunction jettyStartFunction = new JettyFallbackStartFunction(inputMap);
        sc.parallelize(port).foreach(jettyStartFunction);

    }
}
