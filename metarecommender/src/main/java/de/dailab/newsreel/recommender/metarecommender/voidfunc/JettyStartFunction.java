package de.dailab.newsreel.recommender.metarecommender.voidfunc;

import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.common.inter.Recommender;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * This is a void function [ (port) -> () ] which initialize a Eclipse Jetty Web Server.
 *
 */
public class JettyStartFunction implements VoidFunction<Integer> {

    protected AbstractHandler handler;

    public JettyStartFunction(AbstractHandler handler) {
        this.handler  = handler;
    }

    /**
     * (function) call of jetty void function [ (port) -> () ].
     *
     * @param port
     *          Port of Web Server.
     * @throws Exception
     *          If port is already occupied, hostname is not correct or something similar went wrong.
     */
    public void call(Integer port) throws Exception {

        // set up server with host name and port.
        Server server = new Server(new InetSocketAddress(port.intValue()));
        server.setHandler(handler);

        // start server
        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
