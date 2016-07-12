package de.dailab.newsreel.recommender.metarecommender.spark;

import de.dailab.newsreel.recommender.common.util.SharedService;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by domann on 20.02.16.
 */
public class MapToPairRDD {

    private static final Logger log = Logger.getLogger(MapToPairRDD.class);
    // static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    private Int2IntOpenHashMap map = new Int2IntOpenHashMap();

    @Before
    public void setUp() throws Exception {

        // start spark
        SharedService.getInstance();

        // create primitive integer 2 integer map with random entries.
        SecureRandom random = null;
        random = new SecureRandom();
        int streamSize = 100;
        IntStream keysStream = random.ints(streamSize);
        random = new SecureRandom();
        IntStream valuesStream = random.ints(streamSize);

        Iterator<Integer> keyIter   = keysStream.iterator();
        Iterator<Integer> valueIter = valuesStream.iterator();

        for (int i = 0; i < streamSize; i++) {
            map.put(keyIter.next(),valueIter.next());
        }
    }

    @Test
    public void testName() throws Exception {
        log.info(map);
        JavaPairRDD<Integer, Integer> pairRDD = SharedService.getInstance().parallelizePairs(map);
        log.debug(pairRDD.toDebugString());
        assertThat(pairRDD.toArray().size(), is(100));
    }




}
