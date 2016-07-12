package de.dailab.newsreel.recommender;

import de.dailab.newsreel.recommender.common.baseimpl.BasicItem;
import de.dailab.newsreel.recommender.common.defaults.PlistaParameter;
import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.common.inter.Recommender;
import de.dailab.newsreel.recommender.common.util.SharedService;
import de.dailab.newsreel.recommender.util.LimitedList;
import de.dailab.newsreel.recommender.util.SingleThreadExecutor;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import static de.dailab.newsreel.recommender.util.RecommenderHelper.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jens on 16.01.16.
 */
public class CategoryMP implements Recommender {

    private static final Logger log = Logger.getLogger(MostPopular.class);
    private static final int NUM_PARTITIONS = 8;

    //static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    private static final String NAME = "Most Popular Recommender regarding Categories";
    private static final boolean FILTER_TIME_WINDOW = false;
    private int DEFAULT_LIST_LENGTH = 50;
    private long MIN_AVAILABLE_IMPRESSIONS = DEFAULT_LIST_LENGTH / 2L;
    private final Long DEFAULT_CATEGORY = 0L;

    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, LimitedList<Item>>> pool
            = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, ConcurrentHashMap<Long, List<Long>>> results
            = new ConcurrentHashMap<>();
    private HashMap<Long, HashMap<Long, SingleThreadExecutor>> workerPool
            = new HashMap<>();

    private long TIME_WINDOW_MS = 1000 * 60 * 60 * 1;

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * constructor to use all the default values
     */
    public CategoryMP () { }

    /**
     * initialize the recommender with provided list length
     * that stores the incoming impressions
     * @param listLength must be positive. values above 200 are not reasonable for plista
     */
    public CategoryMP (int listLength, int timewindow) {
        if (listLength < 1) {
            log.error("Illegal Argument for list length in MostPopular-Recommender");
            return;
        }
        DEFAULT_LIST_LENGTH = listLength;
        TIME_WINDOW_MS *= timewindow;
    }

    @Override
    public void update(Item item, Long domainID) {
        if (item == null) return;
        if (item.getItemID() == null
                || item.getItemID() == 0L) return;
        if (domainID == null) return;

        Set<Long> ids =  new HashSet<>();
        if (item.getCategoryIds() != null && item.getCategoryIds().size() != 0) {
            ids.addAll(item.getCategoryIds());
        }
        ids.add(DEFAULT_CATEGORY);

        for (Long categoryId : ids)
            learn(domainID, categoryId, item);
    }

    private void learn(Long domainID, Long categoryId, Item item) {
        if (!pool.containsKey(domainID)) {
            ConcurrentHashMap<Long, LimitedList<Item>>
                    cats = new ConcurrentHashMap<>();
            pool.put(domainID, cats);
        }
        if (!pool.get(domainID).containsKey(categoryId)) {
            LimitedList<Item> list = new LimitedList<>(DEFAULT_LIST_LENGTH);
            pool.get(domainID).put(categoryId, list);
        }
        pool.get(domainID).get(categoryId).add(item);
        if (!workerPool.containsKey(domainID)) {
            HashMap<Long, SingleThreadExecutor> catWorker = new HashMap<>();
            workerPool.put(domainID, catWorker);
        }
        if (!workerPool.get(domainID).containsKey(categoryId)) {
            final SingleThreadExecutor worker = new SingleThreadExecutor();
            workerPool.get(domainID).put(categoryId, worker);
        }
        workerPool.get(domainID).get(categoryId).submit(new Thread() {
            @Override
            public void run() {
                try {
                    updateModel(domainID, categoryId);
                } catch (Exception e) {
                    log.error("failed to update model", e);
                }
            }
        });
    }

    /**
     * to reduce task sizes, all spark tasks need to access local variables only.
     * A transparent access over >this< forces spark to serialize the parent object.
     * To mark this in the implementation, all variables with forced local
     * access have a '_' prefix.
     * @param domainID
     * @param categoryId
     */
    private void updateModel(Long domainID, Long categoryId) {
        if (categoryId == null) categoryId = 0L;
        ArrayList<Item> _items = new ArrayList(pool.get(domainID).get(categoryId));
        _items.removeAll(Collections.singleton(null));
        JavaRDD<Item> rdd = SharedService.parallelize(_items);

        long _deadline = System.currentTimeMillis() - TIME_WINDOW_MS;
        int _num_partitions = NUM_PARTITIONS;
        try {
            if (FILTER_TIME_WINDOW) {
                long available = rdd.filter(i -> i.getTimestamp() > _deadline)
                        .count();
                if (available > MIN_AVAILABLE_IMPRESSIONS) {
                    rdd = rdd.filter(i -> i.getTimestamp() > _deadline);
                }
            }
        } catch (Exception e) {
            log.error("worker failed - counting available impressions", e);
        }

        if (!results.containsKey(domainID)) {
            ConcurrentHashMap<Long, List<Long>>
                    cats = new ConcurrentHashMap<>();
            results.put(domainID, cats);
        }
        try {
            results.get(domainID).put(categoryId, rdd
                    .mapToPair(i -> new Tuple2<>(i.getItemID(), 1))
                    .reduceByKey((v1, v2) -> v1 + v2, _num_partitions)
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false, _num_partitions)
                    .map(Tuple2::_2)
                    .collect());
        } catch (Exception e) {
            log.error("worker failed - updating results set", e);
        }
    }

    @Override
    public List<Long> predict(Item item, Long domainID, Integer numberOfRequestedResults) {
        boolean dirtyItem = false;
        List<Long> result = new ArrayList<>();
        if (item == null || item.getItemID() == null) {
            item = new BasicItem(0L, 0L);
            dirtyItem = true;
        }
        if (numberOfRequestedResults == null) {
            numberOfRequestedResults = PlistaParameter.DEFAULT_NUMBER_OF_PREDICTIONS;
        }

        if (domainID == null) return predict_fallback(item, null, numberOfRequestedResults);
        if (!dirtyItem)
            update(item, domainID);
        if (results.get(domainID) == null
                || results.get(domainID).isEmpty())
            return predict_fallback(item, domainID, numberOfRequestedResults);

        ArrayList<Long> catIds = new ArrayList<>();
        if (item.getCategoryIds() != null && item.getCategoryIds().size() != 0) {
            catIds.addAll(item.getCategoryIds());
        }
        catIds.add(DEFAULT_CATEGORY);

        ArrayList<List<Long>> bins = new ArrayList<>();
        int maxLength = 0;
        for (Long catID : catIds) {
            int lenght = results.get(domainID).get(catID).size();
            maxLength = (maxLength > lenght) ? maxLength : lenght;
            bins.add(new ArrayList<>(results.get(domainID).get(catID)));
        }

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

        int size = result.size();
        boolean fill = size < numberOfRequestedResults;

        if (fill) {
            result.addAll(predict_fallback(item, domainID, numberOfRequestedResults-size));
            return result;
        } else {
            return result;
        }
    }



}
