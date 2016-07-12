package de.dailab.newsreel.recommender.util;

import de.dailab.newsreel.recommender.common.util.SharedService;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The class implements a thread-safe LinkedHashMap with a maximalSize. The keys are stored in insert-order
 * In contrast to other concurrent map implementations, the size operation is fast (constant in the mapSize),
 * but might imprecise. The size is updated in fixed intervals.
 * <p/>
 * Unfortunately the thread-safeness is difficult to test.
 * <p/>
 * The skipList is currently not implemented.
 *
 * @author jaschar
 */
public class ConcurrentSparkList implements Serializable{

    private static final Logger log = Logger.getLogger(ConcurrentSparkList.class);
    static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    private final Long2LongOpenHashMap data;
    private JavaPairRDD<Long, Long> item2ReadCount;
    private JavaPairRDD<Long, Long> item2timeStampData;

    private AtomicInteger atomicInteger;

    private Function2<Long, Long, Long> replaceValues = ((Function2<Long, Long, Long> & Serializable) (x, y) -> {
        if (x > y) {
            return x;
        } else {
            return y;
        }
    });

    private int maxSize = 10;
    private int numPartitions;

    /**
     * Constructor
     *
     * @param _maxSize set the maximal size
     */
    public ConcurrentSparkList(final int _maxSize) {

        // start spark node
        SharedService.getInstance();

        // check the parameter
        if (_maxSize <= 2) {
            throw new IllegalArgumentException("maxSize must not be <= 2");
        }

        atomicInteger = new AtomicInteger();
        atomicInteger.set(0);

        // set the parameters
        this.maxSize = _maxSize;

        data = new Long2LongOpenHashMap(maxSize);
        item2ReadCount = SharedService.parallelizePairs(data);
        item2timeStampData = SharedService.parallelizePairs(data);
        numPartitions = item2ReadCount.context().defaultParallelism();

    }

    public boolean isEmpty() {
        return item2ReadCount.isEmpty();
    }

    private void put(Long key, Long value) {

        JavaPairRDD<Long, Long> filteredPairRDD = item2ReadCount.filter(t -> t._1().equals(key));

        if(filteredPairRDD.isEmpty()) {
            atomicInteger.getAndIncrement();
        }

        JavaPairRDD<Long, Long> newPair = SharedService.parallelizePairs(new Tuple2(key, value));
        JavaPairRDD<Long, Long> timeStamp = SharedService.parallelizePairs(new Tuple2(key, System.currentTimeMillis()));

        if (size() < maxSize) {
            addNewElement(newPair, timeStamp);
        } else {
            // remove a old key
            removeOldKey();

            // add new pair
            addNewElement(newPair, timeStamp);
        }

    }

    private void removeOldKey() {
        remove(getOldest());
    }

    private void addNewElement(JavaPairRDD newPair, JavaPairRDD timeStamp) {
        item2ReadCount = item2ReadCount
                .union(newPair)
                .coalesce(numPartitions, false)
                .reduceByKey((v1, v2) ->  (Long) v1 +  (Long) v2, numPartitions)
                .mapToPair((PairFunction<Tuple2<Long, Long>, Long, Long>) Tuple2::swap)
                .sortByKey(false, numPartitions)
                .mapToPair((PairFunction<Tuple2<Long, Long>, Long, Long>) Tuple2::swap);
        item2timeStampData = item2timeStampData
                .union(timeStamp)
                .coalesce(numPartitions, false)
                .reduceByKey(replaceValues)
                .mapToPair((PairFunction<Tuple2<Long, Long>, Long, Long>) Tuple2::swap)
                .sortByKey(true, numPartitions)
                .mapToPair((PairFunction<Tuple2<Long, Long>, Long, Long>) Tuple2::swap);
    }

    public Long remove(Object key) {

        Long oldValue = item2ReadCount
                .collectAsMap()
                .get(key);
        if (oldValue != null) {
            item2ReadCount = item2ReadCount.filter((Function<Tuple2<Long, Long>, Boolean>) t -> !t._1().equals(oldValue));
        }
        return oldValue;
    }

    public int size() {
        return atomicInteger.get();
    }

    public void incrementKeyFrequency(final Long key) {
        incrementKeyFrequency(key, 1);
    }

    /**
     * @param key
     */
    private void incrementKeyFrequency(final Long key, final long count) {
        put(key, count);
    }

    public List<Long> deliver() {
            return item2ReadCount
                    .map(t -> t._1())
                    .collect();
    }

    Tuple2<Long, Long> getOldest() {
        return item2timeStampData.first();
    }

    Tuple2<Long, Long> getNewest() {
        System.out.println("getNewest is inefficient, do not use it!!! it is just for debug proposes.");
        return item2timeStampData.collect().get((int) item2timeStampData.count() - 1);
    }

    /**
     *
     */
    public Iterator<Tuple2<Long, Long>> getIterator() {
        return item2ReadCount.collect().iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (Tuple2<Long, Long> entry : this.item2ReadCount.collect()) {
            sb.append("(" + entry._1() + "|" + entry._2() + ")");
        }
        sb.append("] FIFO=>");
        for (Long entry : this.item2ReadCount.keys().collect()) {
            sb.append(entry + ", ");
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        ConcurrentSparkList me = new ConcurrentSparkList(3);
        me.incrementKeyFrequency(6L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(1L, 2L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(2L, 2L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(2L, 2L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(2L, 2L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(1L, 2L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(1L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(1L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(1L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(3L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(3L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(1L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(1L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(3L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(4L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(1L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(5L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        me.incrementKeyFrequency(5L, 1L);
        System.out.println(me);
        System.out.println(me.size());
        System.out.println("start the iterator (sort by value descending)");
        for (Iterator<Tuple2<Long, Long>> iterator = me.getIterator(); iterator.hasNext(); ) {
            Tuple2<Long, Long> entry = iterator.next();
            System.out.println(entry._1() + "||" + entry._2());
        }

        System.out.println("get oldest tuple:");
        System.out.println(me.getOldest());
        System.out.println("get newest tuple:");
        System.out.println(me.getNewest());

        System.out.println("deliver ranking:");
        System.out.println(me.deliver());
    }
}
