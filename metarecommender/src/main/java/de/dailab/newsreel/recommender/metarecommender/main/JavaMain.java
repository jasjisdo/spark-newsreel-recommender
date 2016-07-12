package de.dailab.newsreel.recommender.metarecommender.main;

import com.google.common.collect.*;
import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.common.inter.Recommender;

import de.dailab.newsreel.recommender.common.util.ConfigLoader;
import de.dailab.newsreel.recommender.common.util.SharedService;
import de.dailab.newsreel.recommender.metarecommender.delegate.RequestDelegateHandler;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import de.dailab.newsreel.recommender.metarecommender.voidfunc.JettyStartFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Spark (java) main class which starts a meta recommender based on jetty web server.
 *
 * Created by domann on 17.11.15.
 */
public class JavaMain {

    private static final Logger log = Logger.getLogger(JavaMain.class);
    // static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    public static void main(String[] args) {


        /*
         * initialize the config file.
         */
        ConfigLoader.Conf config = null;
        try {
            config = ConfigLoader.load();
        } catch (IOException e) {
            e.printStackTrace();
            if( !(config instanceof ConfigLoader.Conf) ) {
                System.exit(1);
            }
        }

        /*
         * Initialize spark context which representing the spark app/job.
         */
        startContext(Integer.parseInt(config.getNumberOfWorkers()));

        /*
         * Set embedded jetty port by config.
         */
        int configPort = Integer.parseInt(config.getPort());
        List<Integer> port = Arrays.asList(configPort);

        /*
         * Use custom void function to start jetty sever.
         */
        JettyStartFunction jettyStartFunction = new JettyStartFunction(new RequestDelegateHandler());

        /*
         * This is a hack to apply/run a spark job which initialize a Eclipse Jetty Server in Spark.
         * foreach of 1 element list looks strange, but it is needed to run a job as service.
         */
        log.debug("classpath: " + printClassPath());
        log.debug(JavaMain.class.getResource("/config.properties").getFile());
        log.debug("run server ...");

        /* START JETTY INSIDE SPARK, CURRENTLY NOT USED BECAUSE OF PORT BINDING ISSUES
        SharedService.getInstance().parallelize(port).foreach(jettyStartFunction); */
        try {
            jettyStartFunction.call(port.get(0));
        } catch (Exception e) {
            log.error("failed to start jetty", e);
        }
    }

    /**
     * Starts the spark context given a valid configuration.
     * starts a test map-reduce such that all spark workers can fetch dependencies in advance
     */
    private static void startContext(int numOfWorkers) {
        JavaSparkContext sc = SharedService.getContext();

        for (int i=0; i<numOfWorkers;i++) {
            final int threadnumber = i;
            new Thread(){
                @Override
                public void run() {
                    ImmutableList<Integer> range =
                            ContiguousSet.create(Range.closed(1, 5), DiscreteDomain.integers()).asList();

                    JavaRDD<Integer> data = sc.parallelize(range).repartition(numOfWorkers);
                    Integer result = data.reduce((Function2<Integer, Integer, Integer>)
                            (v1, v2) -> v1 + v2);
                    if (result == 15)
                        log.info("successfully tested worker"+threadnumber);
                    else
                        log.warn("worker "+threadnumber+" yielded a false result: "
                                +result+" (should be 15)");
                }
            }.start();
        }
    }

    public static String printClassPath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader)cl).getURLs();
        StringBuilder stringBuilder = new StringBuilder();
        for(URL url: urls){
            stringBuilder.append(url.getFile() + "\n");
        }
        return stringBuilder.toString();
    }
}
