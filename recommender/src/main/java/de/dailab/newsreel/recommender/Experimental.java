package de.dailab.newsreel.recommender;

import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.common.inter.Recommender;
import de.dailab.newsreel.recommender.common.util.SharedService;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by jens on 16.01.16.
 */
public class Experimental implements Recommender {

    private ArrayList<Item> pool = new ArrayList<>();

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void update(Item item, Long domainID) {
        if (item == null) return;
        if (item.getItemID() == null || item.getItemID() == 0) return;
        if (item.getUserID() == null || item.getUserID() == 0) return;

        pool.add(item);
    }

    @Override
    public List<Long> predict(Item item, Long domainID, Integer numberOfRequestedResults) {
        return null;
    }


    public void eval() {
        List<Tuple2<Integer, Integer>> list = SharedService.parallelize(pool)
                .mapToPair(i -> new Tuple2<>(i.getUserID(), 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(i -> new Tuple2<>(i._2(), 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .sortByKey(false)
                .collect();
        list.forEach(i -> System.out.println("i = " + i));
    }
}
