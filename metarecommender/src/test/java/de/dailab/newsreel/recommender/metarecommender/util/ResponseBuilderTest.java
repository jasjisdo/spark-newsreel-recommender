package de.dailab.newsreel.recommender.metarecommender.util;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.omg.PortableInterceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by domann on 20.01.16.
 */
public class ResponseBuilderTest {

    private static final Logger log = Logger.getLogger(ResponseBuilderTest.class);
    // static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    List<Integer> predictions;

    @Before
    public void setUp() throws Exception {
        predictions = Lists.newArrayList(144553, 24466, 46646, 35353354, 244);
    }

    @Test
    public void testName() throws Exception {
        String response = "{\"recs\": {\"ints\": {\"3\": ".concat(predictions.toString()).concat(" } } }");
        log.info(response);
    }
}
