package de.dailab.newsreel.recommender.common.util;


import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jens on 10.12.15.
 *
 * Find the configuration file in src/main/resources/config.properties
 *
 * The fields can be defined in the nested Conf class.
 * Define the member and respective setter and getter.
 * Defaults can be realized by initialization values.
 */
public class ConfigLoader {

    protected static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);

    private static final String FILENAME = "config.properties";

    private static Conf _instance = null;

    public static Conf load() throws IOException {
        if (_instance != null) {
            return _instance;
        }
        // Prepare
        Properties prop = new Properties();

        String filename = new String(FILENAME);
        InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(filename);
        if (input == null) {
            throw new IOException("Sorry, unable to find " + filename);
        }

        // load a properties file
        prop.load(input);

        try {
            input.close();
        } catch (IOException e) {
            log.error("could not close configurations file", e);
        }
        _instance = new Conf(prop);
        return _instance;
    }

    /**
     * This is the nested class where the configuration fields
     * are defines as member.
     */
    public static class Conf{

        /* define configuration fields here. Don't forget getter/setter.
         * The values initiated here are handled as defaults */
        private String appName = "App";
        private String master  = "local";
        private String driverHost  = "localhost";
        private String driverPort = "50916";
        private String port    = "8080";
        private String maxMem  = "4g";
        private String driverMem = "4g";
        private String exeMem = "2900m";
        private String exeCores = "2";
        private String eventlogging = "false";
        private String eventlogDir = "file:///tmp/spark-events";
        private String jarFolder = "/lib/metarecommender-0.0.1-SNAPSHOT.jar";
        private String localIp = "10.8.0.1";
        private String fileServerPort = "59255";
        private String fileServerURI = "http://10.8.0.1:59255";
        private String numberOfWorkers = "8";

        public Conf(Properties properties){
            Map<String, String> map = new HashMap<>();
            for (final String name: properties.stringPropertyNames())
                map.put(name, properties.getProperty(name));
            try {
                BeanUtils.populate(this, map);
            } catch (IllegalAccessException e) {
                log.error("error building config ", e);
            } catch (InvocationTargetException e) {
                log.error("error building config ", e);
            }
        }

        /* The getter/setter follow the normal spring conventions */
        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getMaster() {
            return master;
        }

        public void setMaster(String master) {
            this.master = master;
        }

        public String getDriverHost() {
            return driverHost;
        }

        public void setDriverHost(String driverHost) {
            this.driverHost = driverHost;
        }

        public String getDriverPort() {
            return driverPort;
        }

        public void setDriverPort(String driverPort) {
            this.driverPort = driverPort;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getMaxMem() {
            return maxMem;
        }

        public void setMaxMem(String maxMem) {
            this.maxMem = maxMem;
        }

        public String getDriverMem() {
            return driverMem;
        }

        public void setDriverMem(String driverMem) {
            this.driverMem = driverMem;
        }

        public String getExeMem() {
            return exeMem;
        }

        public void setExeMem(String exeMem) {
            this.exeMem = exeMem;
        }

        public String getEventlogging() {
            return eventlogging;
        }

        public void setEventlogging(String eventlogging) {
            this.eventlogging = eventlogging;
        }

        public String getEventlogDir() {
            return eventlogDir;
        }

        public void setEventlogDir(String eventlogDir) {
            this.eventlogDir = eventlogDir;
        }

        public String getJarFolder() {
            return jarFolder;
        }

        public void setJarFolder(String jarFolder) {
            this.jarFolder = jarFolder;
        }

        public String getExeCores() {
            return exeCores;
        }

        public void setExeCores(String exeCores) {
            this.exeCores = exeCores;
        }

        public String getLocalIp() {
            return localIp;
        }

        public void setLocalIp(String localIp) {
            this.localIp = localIp;
        }

        public String getFileServerPort() {
            return fileServerPort;
        }

        public void setFileServerPort(String fileServerPort) {
            this.fileServerPort = fileServerPort;
        }

        public String getFileServerURI() {
            return fileServerURI;
        }

        public void setFileServerURI(String fileServerURI) {
            this.fileServerURI = fileServerURI;
        }

        public String getNumberOfWorkers() {
            return numberOfWorkers;
        }

        public void setNumberOfWorkers(String numberOfWorkers) {
            this.numberOfWorkers = numberOfWorkers;
        }
    }
}
