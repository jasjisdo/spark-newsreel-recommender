package de.dailab.newsreel.recommender;

import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import de.dailab.newsreel.recommender.common.item.Item;
import de.dailab.newsreel.recommender.common.recommender.Recommender;
import de.dailab.newsreel.recommender.common.util.SharedService;
import de.dailab.newsreel.recommender.util.LimitedList;
import de.dailab.newsreel.recommender.util.SingleThreadExecutor;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Tuple2;
import static de.dailab.newsreel.recommender.util.RecommenderHelper.*;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * EXPERIMENTAL
 *
 * Created by jens on 19.01.16.
 */
@Deprecated
public class FP implements Recommender {

    private static final Logger log = Logger.getLogger(MostPopular.class);

    static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    private final String NAME = "Frequent Pattern Recommender";

    private static final int LIST_LENGTH = 600;
    private final long TIME_WINDOW_MS = 1000 * 60 * 60 * 24;
    private static final double MIN_CONFIDENCE = 0.0;
    private static final double MIN_SUPPORT = 0.0;
    private static final int NUM_PARTITIONS = 8;


    private ConcurrentHashMap<Long, LimitedList<Item>> pool = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, HashMap<Long, Set<Long>>> results = new ConcurrentHashMap<>();
    private HashMap<Long, SingleThreadExecutor> workerPool = new HashMap<>();

    private static FPGrowth fpg = new FPGrowth()
            .setMinSupport(MIN_SUPPORT)
            .setNumPartitions(NUM_PARTITIONS);

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void update(Item item, Long domainID) {
        if (item == null) return;
        if (item.getItemID() == null) return;
        if (domainID == null) return;

        if (pool.get(domainID) == null) {
            LimitedList<Item> list = new LimitedList<>(LIST_LENGTH);
            list.add(item);
            pool.put(domainID, list);
            final SingleThreadExecutor worker = new SingleThreadExecutor();
            workerPool.put(domainID, worker);
        }
        else {
            learn(domainID, item);
        }
    }

    private void learn(Long domainID, Item item) {
        pool.get(domainID).add(item);
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
        List<Item> _items = pool.get(domainID).sorted();
        JavaRDD<Item> rdd = SharedService.parallelize(_items);
        if (!results.containsKey(domainID)) results.put(domainID, new HashMap<>());

        long _deadline = System.currentTimeMillis() - TIME_WINDOW_MS;
        try {
            FPGrowthModel<Long> model = fpg.run(rdd
                    .filter(i -> i.getTimestamp() > _deadline)
                    .mapToPair(item -> new Tuple2<>(item.getUserID(), item.getItemID()))
                    .groupByKey()
                    .map(t -> toCollection(t._2())));
            for (AssociationRules.Rule<Long> rule
                    : model.generateAssociationRules(MIN_CONFIDENCE).toJavaRDD().collect()) {
                Long key = rule.javaAntecedent().get(0);
                Long val = rule.javaConsequent().get(0);
                if (results.get(domainID).containsKey(key)) {
                    results.get(domainID).get(key).add(val);
                } else {
                    HashSet<Long> set = new HashSet<>();
                    set.add(val);
                    results.get(domainID).put(key, set);
                }
            }
        }catch (Exception e){
            log.error(">>> rdd: " + rdd.toDebugString());
            log.error("worker failed", e);
        }
    }

    @Override
    public List<Long> predict(Item item, Long domainID, Integer numberOfRequestedResults) {
        if (item == null) return null;
        if (numberOfRequestedResults == null) return null;
        if (item.getItemID() == null) return null;
        if (domainID == null) return predict_fallback(item, null, numberOfRequestedResults);
        update(item, domainID);
        if (results.get(domainID) == null
                || results.get(domainID).isEmpty())
            return predict_fallback(item, domainID, numberOfRequestedResults);

        int size = 0;
        if (results.get(domainID).get(item.getItemID()) != null) {
           size = results.get(domainID).get(item.getItemID()).size();
        }
        boolean fill = size < numberOfRequestedResults;
        int n = (fill)?
                size : numberOfRequestedResults;
        List<Long> result = new ArrayList<>();
        for (int i=0; i<n; i++) {
            result.addAll(results.get(domainID).get(item.getItemID()));
        }

        if (fill) {
            result.addAll(predict_fallback(item, domainID, numberOfRequestedResults-size));
            return result;
        } else {
            return result;
        }
    }


    /**
     * Copies an iterable's elements into an array.
     *
     * @param iterable the iterable to copy
     * @param type the type of the elements
     * @return a newly-allocated array into which all the elements of the iterable
     *     have been copied
     */
    public static <T> T[] toArray(Iterable<? extends T> iterable, Class<T> type) {
        Collection<? extends T> collection = toCollection(iterable);
        T[] array = ObjectArrays.newArray(type, collection.size());
        return collection.toArray(array);
    }

    /**
     * Returns a new array of the given length with the specified component type.
     *
     * @param type the component type
     * @param length the length of the new array
     */
    @SuppressWarnings("unchecked")
    static <T> T[] newArray(Class<T> type, int length) {
        return (T[]) Array.newInstance(type, length);
    }

    /**
     * Converts an iterable into a collection. If the iterable is already a
     * collection, it is returned. Otherwise, an {@link java.util.ArrayList} is
     * created with the contents of the iterable in the same iteration order.
     */
    private static <E> Collection<E> toCollection(Iterable<E> iterable) {
        return (iterable instanceof Collection)
                ? Sets.newHashSet(iterable)
                : Sets.newHashSet(iterable.iterator());
    }
}
