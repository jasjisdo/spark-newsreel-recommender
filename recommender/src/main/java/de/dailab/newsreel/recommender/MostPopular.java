package de.dailab.newsreel.recommender;

import de.dailab.newsreel.recommender.common.baseimpl.BasicItem;
import de.dailab.newsreel.recommender.common.defaults.PlistaParameter;
import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.common.inter.Recommender;
import de.dailab.newsreel.recommender.common.util.EvalLevel;
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
public class MostPopular implements Recommender {

    private static final Logger log = Logger.getLogger(MostPopular.class);
    private static final int NUM_PARTITIONS = 8;

    private static final Logger evallog = Logger.getLogger(EvalLevel.class);
    static {evallog.setLevel(EvalLevel.EVAL);}

    private static final String NAME = "Most Popular Recommender";
    private static final boolean FILTER_TIME_WINDOW = false;
    private int DEFAULT_LIST_LENGTH = 50;
    private long MIN_AVAILABLE_IMPRESSIONS = DEFAULT_LIST_LENGTH / 2L;

    private ConcurrentHashMap<Long, LimitedList<Item>> pool = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, List<Long>> results = new ConcurrentHashMap<>();
    private HashMap<Long, SingleThreadExecutor> workerPool = new HashMap<>();

    private long TIME_WINDOW_MS = 1000 * 60 * 60 * 1;

    private volatile long jobcounter = 0;

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * constructor to use all the default values
     */
    public MostPopular () {
        Thread scheduler = new Thread() {
            @Override
            public void run() {
                while (true) {
                    evallog.log(EvalLevel.EVAL,
                            "jobcounter\t" + System.currentTimeMillis() + "\t" + jobcounter);
                    try {
                        Thread.sleep(1000*10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        scheduler.start();
    }

    /**
     * initialize the recommender with provided list length
     * that stores the incoming impressions
     * @param listLength must be positive. values above 200 are not reasonable for plista
     */
    public MostPopular (int listLength, int timewindow) {
        if (listLength < 1) {
            log.error("Illegal Argument for list length in MostPopular-Recommender");
            return;
        }
        DEFAULT_LIST_LENGTH = listLength;
        TIME_WINDOW_MS *= timewindow;
    }

    /**
     * prove a map of domain ids and respective maximum list length to store the impressions
     * @param lengthlist
     */
    public MostPopular (Map<Long, Integer> lengthlist, int timewindow) {
        if (lengthlist == null) return;
        for (Long key : lengthlist.keySet()) {
            Integer length = lengthlist.get(key);
            if (length > 0) {
                LimitedList<Item> list = new LimitedList<>(length);
                pool.put(key, list);
                final SingleThreadExecutor worker = new SingleThreadExecutor();
                workerPool.put(key, worker);
            } else {
                log.error("Illegal Argument for list length in MostPopular-Recommender for domain "+key);
            }
        }
        TIME_WINDOW_MS *= timewindow;
    }

    @Override
    public void update(Item item, Long domainID) {
        if (item == null) return;
        if (item.getItemID() == null
                || item.getItemID() == 0) return;
        if (domainID == null) return;

        learn(domainID, item);
    }

    private void learn(Long domainID, Item item) {
        if (!pool.containsKey(domainID)) {
            LimitedList<Item> list = new LimitedList<>(DEFAULT_LIST_LENGTH);
            pool.put(domainID, list);
        }
        pool.get(domainID).add(item);
        if (!workerPool.containsKey(domainID)) {
            log.error("the domainId "
                    .concat(domainID.toString())
                    .concat(" seems to be unknown yet."));
            final SingleThreadExecutor worker = new SingleThreadExecutor();
            workerPool.put(domainID, worker);
        }
        workerPool.get(domainID).submit(new Thread() {
            @Override
            public void run() {
                updateModel(domainID);
            }
        });
    }

    /**
     * to reduce task sizes, all spark tasks need to access local variables only.
     * A transparent access over >this< forces spark to serialize the parent object.
     * To mark this in the implementation, all variables with forced local
     * access have a '_' prefix.
     * @param domainID
     */
    private void updateModel(Long domainID) {
        ArrayList<Item> _items = new ArrayList(pool.get(domainID));
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

        try {
            results.put(domainID, rdd
                    .mapToPair(i -> new Tuple2<>(i.getItemID(), 1))
                    .reduceByKey((v1, v2) -> v1 + v2, _num_partitions)
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false, _num_partitions)
                    .map(Tuple2::_2)
                    .collect());
            jobcounter++;
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

        int size = results.get(domainID).size();
        boolean fill = size < numberOfRequestedResults;
        int n = (fill)?
                size : numberOfRequestedResults;
        for (int i=0; i<n; i++) {
            result.add(results.get(domainID).get(i));
        }

        if (fill) {
            result.addAll(predict_fallback(item, domainID, numberOfRequestedResults-size));
            return result;
        } else {
            return result;
        }
    }



}
