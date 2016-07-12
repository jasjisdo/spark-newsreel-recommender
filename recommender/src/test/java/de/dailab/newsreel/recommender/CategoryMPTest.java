package de.dailab.newsreel.recommender;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

/**
 * Created by jens on 26.02.16.
 */
public class CategoryMPTest {

    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, List<Long>>> results
            = new ConcurrentHashMap<>();
    private Long domainID = 42L;
    private ArrayList<Long> catIds = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        results = new ConcurrentHashMap<>();
        catIds = new ArrayList<>();
        domainID = 42L;

        catIds.add(0L);
        catIds.add(3L);
        results.put(domainID, new ConcurrentHashMap<>());
        for (Long catId : catIds) {
            results.get(domainID).put(catId,new ArrayList<>());
            results.get(domainID).get(catId).addAll(randomArrayList(3));
        }
    }



    @Test
    public void testPredict() throws Exception {
        List<Long> result = new ArrayList<>();
        // make length unequal
        results.get(domainID).get(0L).remove(0);
        // produce duplicates
        results.get(domainID).get(3L).set(0, results.get(domainID).get(0L).get(0));

        ArrayList<List<Long>> bins = new ArrayList<>();
        int maxLength = 0;
        for (Long catID : catIds) {
            int lenght = results.get(domainID).get(catID).size();
            maxLength = (maxLength > lenght) ? maxLength : lenght;
            bins.add(new ArrayList<>(results.get(domainID).get(catID)));
        }

        int numberOfRequestedResults = 4;
        // round robin
        for (int i = 0; i < maxLength * bins.size(); i++) {
            int j = i % bins.size();
            int k = i / bins.size();
            try {
                Long newEntry = bins.get(j).get(k);
                if (!result.contains(newEntry))
                    result.add(bins.get(j).get(k));
                if (result.size() >= numberOfRequestedResults)
                    break;
            } catch (Exception e) {
                // do not care
            }
        }
        System.out.println("results = \n" + results);
        System.out.println("result = \n" + result);
    }

    public ArrayList<Long> randomArrayList(int n)
    {
        ArrayList<Long> list = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < n; i++)
        {
            list.add(random.nextLong());
        }
        return list;
    }

}