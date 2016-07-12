package de.dailab.newsreel.recommender.metarecommender.voidfunc;

import de.dailab.newsreel.recommender.common.item.Item;
import de.dailab.newsreel.recommender.common.recommender.Recommender;
import de.dailab.plistacontest.client.ContestHandler;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.eclipse.jetty.server.Server;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

/**
 * Created by domann on 17.11.15.
 */
public class JettyFallbackStartFunction implements VoidFunction<Integer> {

    private Server server;

    private Map<Recommender, JavaRDD<? extends Item> > inputMap;

    public JettyFallbackStartFunction(Map<Recommender, JavaRDD<? extends Item> > inputMap) {
        this.inputMap = inputMap;
    }

    public void call(Integer port) throws Exception {

        // store some configurations
        final Properties properties = new Properties();

        String hostname = "0.0.0.0";

        // set up server
        server = new Server(new InetSocketAddress(hostname, port.intValue()));
        server.setHandler(new ContestHandler(null, null));

        // start server
        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
