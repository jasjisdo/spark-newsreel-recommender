package de.dailab.newsreel.recommender;

import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.common.inter.Recommender;
import de.dailab.newsreel.recommender.common.util.SharedService;
import de.dailab.newsreel.recommender.util.LimitedList;
import de.dailab.newsreel.recommender.util.SingleThreadExecutor;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static de.dailab.newsreel.recommender.util.RecommenderHelper.predict_fallback;

/**
 * Created by jens on 20.01.16.
 */
public class CF implements Recommender {

    private static final Logger log = Logger.getLogger(CF.class);
    private static final int NUM_PARTITIONS = 8;

    //static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    private static final String NAME = "Alternate Least Squares Recommender";
    private static final int LIST_LENGTH = 200;
    private static final int RANK = 5;
    private static final int ITERATIONS = 10;
    private static final double LAMBDA = 0.1;

    private ConcurrentHashMap<Long, LimitedList<Tuple2>> pool = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, MatrixFactorizationModel> results = new ConcurrentHashMap<>();
    private HashMap<Long, SingleThreadExecutor> workerPool = new HashMap<>();

    private final long TIME_WINDOW_MS = 1000 * 60 * 60 * 24;

    private final boolean ITEM_BASED = false;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void update(Item item, Long domainID) {
        if (item == null) return;
        if (item.getItemID() == null
                || item.getItemID() == 0) return;
        if (domainID == null) return;
        Tuple2 tuple = new Tuple2(item.getUserID(), item.getItemID());
        if (ITEM_BASED) tuple.swap();

        learn(domainID, tuple);
    }

    private void learn(Long domainID, Tuple2 tuple) {
        if (!pool.containsKey(domainID)) {
            LimitedList<Tuple2> list = new LimitedList<>(LIST_LENGTH);
            pool.put(domainID, list);
        }
        pool.get(domainID).add(tuple);
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
        ArrayList<Rating> _items = new ArrayList<>();
        LimitedList<Tuple2> list = pool.get(domainID);
        int max = list.size();
        for (int i = 0; i < max; i++) {
            Tuple2 t = list.get(i);
            _items.add(new Rating(((Long)t._1()).intValue(),((Long)t._2()).intValue(),1));
        }
        JavaRDD<Rating> rdd = SharedService.parallelize(_items);

        try {
            MatrixFactorizationModel model = ALS.train(rdd.rdd(), RANK, ITERATIONS, LAMBDA);
            results.put(domainID, model);
        }catch (Exception e){
            log.error("worker failed", e);
        }
    }

    @Override
    public List<Long> predict(Item item, Long domainID, Integer numberOfRequestedResults) {
        if (item == null) return null;
        if (numberOfRequestedResults == null) return null;
        if (domainID == null) return predict_fallback(item, null, numberOfRequestedResults);
        update(item, domainID);
        if (results.get(domainID) == null)
            return predict_fallback(item, domainID, numberOfRequestedResults);

        Rating[] ratings = new Rating[]{};
        try {
            if (ITEM_BASED)
                ratings = results.get(domainID).recommendProducts(item.getItemID().intValue(), numberOfRequestedResults);
            else
                ratings = results.get(domainID).recommendProducts(item.getUserID().intValue(), numberOfRequestedResults);
        } catch (Exception e) {
            //log.error("failed recommendation",e);
        }
        int size = ratings.length;
        boolean fill = size < numberOfRequestedResults;
        int n = (fill)?
                size : numberOfRequestedResults;
        List<Long> result = new ArrayList<>();
        for (int i=0; i<n; i++) {
            result.add(new Long(ratings[i].product()));
        }

        if (fill) {
            result.addAll(predict_fallback(item, domainID, numberOfRequestedResults-size));
            return result;
        } else {
            return result;
        }
    }
}
