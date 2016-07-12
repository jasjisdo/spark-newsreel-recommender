import de.dailab.newsreel.recommender.*;
import de.dailab.newsreel.recommender.common.extimpl.IdomaarItem;
import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.common.inter.Recommender;
import de.dailab.newsreel.recommender.common.util.SharedService;
import de.dailab.newsreel.recommender.CF;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by domann on 22.12.15.
 */
public class RecommenderTest {

    private static GZIPInputStream gzin;
    private List<Pair<String, Item>> testSetMap;

    @Before
    public void setUp() throws Exception {
        gzin = new GZIPInputStream(this.getClass().getResourceAsStream("/dummy.data.idomaar.txt.gz"));
        testSetMap = readStream(gzin);
    }

    private List<Pair<String, Item>> readStream(InputStream gzin) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();

        BufferedInputStream bin = new BufferedInputStream(gzin);
        BufferedReader bfr = new BufferedReader(new InputStreamReader(gzin, "UTF-8"));

        List<Pair<String, Item>> items = new ArrayList<>();
        String line = null;
        while ((line = bfr.readLine()) != null) {
            String[] entries = line.split("\\t");
            String requestType = entries[0];
            String properties = entries[3];
            String entities = entries[4];
            IdomaarItem item = new IdomaarItem(properties, entities);
            Pair<String, Item> pair = new ImmutablePair<>(requestType, item);
            items.add(pair);
        }
        return items;
    }

    @Ignore("model is not ready yet")
    @Test
    public void testCollab() throws Exception {
        Recommender recommender = new CollabFilter();
        for (Pair<String, Item> currentPair : testSetMap) {
            IdomaarItem item = (IdomaarItem) currentPair.getValue();
            if (item instanceof IdomaarItem) {
                switch (currentPair.getKey()) {
                    
                    case "impression":

                        recommender.update(item, item.getDomainId());

                        break;

                    case "recommendation":

                        recommender.predict(item, item.getDomainId(), 3);

                        break;
                }
            }
        }
    }

    private String fill(String s, int l) {
        int d = l - s.length();
        for (int i = 0; i<d; i++) {
            s += ' ';
        }
        return s;
    }

    @Ignore("for debugging only")
    @Test
    public void testMostPopular() throws Exception {
        SharedService.getInstance();
        Recommender recommender = new MostPopular();
        for (Pair<String, Item> currentPair : testSetMap) {
            Thread.sleep(10);
            IdomaarItem item = (IdomaarItem) currentPair.getValue();
            if (item instanceof IdomaarItem) {
                switch (currentPair.getKey()) {

                    case "impression":

                        recommender.update(item, item.getDomainId());
                        break;

                    case "recommendation":
                        Thread.sleep(10);
                        long start = System.currentTimeMillis();
                        List<Long> prediction = recommender.predict(item, item.getDomainId(), 3);
                        long end = System.currentTimeMillis();
                        System.out.println(fill(fill("("+(end - start)+" ms)", 10)+item.getDomainId(), 20)+"= " + prediction);

                        break;
                }
            }
        }
    }

    @Ignore("for debugging only")
    @Test
    public void testFP() throws Exception {
        SharedService.getInstance();
        Recommender recommender = new FP();
        for (Pair<String, Item> currentPair : testSetMap) {
            Thread.sleep(10);
            IdomaarItem item = (IdomaarItem) currentPair.getValue();
            if (item instanceof IdomaarItem) {
                switch (currentPair.getKey()) {

                    case "impression":

                        recommender.update(item, item.getDomainId());
                        break;

                    case "recommendation":
                        Thread.sleep(10);
                        long start = System.currentTimeMillis();
                        List<Long> prediction = recommender.predict(item, item.getDomainId(), 3);
                        long end = System.currentTimeMillis();
                        System.out.println(fill(fill("("+(end - start)+" ms)", 10)+item.getDomainId(), 20)+"= " + prediction);

                        break;
                }
            }
        }
    }

    @Ignore("for debugging only")
    @Test
    public void testCF() throws Exception {
        SharedService.getInstance();
        Recommender recommender = new CF();
        for (Pair<String, Item> currentPair : testSetMap) {
            Thread.sleep(10);
            IdomaarItem item = (IdomaarItem) currentPair.getValue();
            if (item instanceof IdomaarItem) {
                switch (currentPair.getKey()) {

                    case "impression":

                        recommender.update(item, item.getDomainId());
                        break;

                    case "recommendation":
                        Thread.sleep(10);
                        long start = System.currentTimeMillis();
                        List<Long> prediction = recommender.predict(item, item.getDomainId(), 3);
                        long end = System.currentTimeMillis();
                        System.out.println(fill(fill("("+(end - start)+" ms)", 10)+item.getDomainId(), 20)+"= " + prediction);

                        break;
                }
            }
        }
    }


    @Ignore("for debugging only")
    @Test
    public void testExperimental() throws Exception {
        Recommender recommender = new Experimental();
        for (Pair<String, Item> currentPair : testSetMap) {
            IdomaarItem item = (IdomaarItem) currentPair.getValue();
            if (item instanceof IdomaarItem) {
                switch (currentPair.getKey()) {

                    case "impression":

                        recommender.update(item, item.getDomainId());
                        break;

                    case "recommendation":
                        long start = System.currentTimeMillis();
                        List<Long> prediction = recommender.predict(item, item.getDomainId(), 3);
                        long end = System.currentTimeMillis();
                        System.out.println(fill(fill("("+(end - start)+" ms)", 10)+item.getDomainId(), 20)+"= " + prediction);

                        break;
                }
            }

        }
        ((Experimental) recommender).eval();
    }
}
