package de.dailab.newsreel.recommender.util;

import de.dailab.newsreel.recommender.common.inter.Item;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jens on 19.01.16.
 */
public class RecommenderHelper {

    private static final Logger log = Logger.getLogger(RecommenderHelper.class);
    //static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    public static List<Long> predict_fallback(Item item, Long domainID, int numberOfRequestedResults) {
        log.debug("used fallback prediction for domain "+domainID);
        List<Long> result = new ArrayList<>();
        if (item == null) return result;
        if (item.getItemID() == null) return result;

        Long maxID = item.getItemID();
        for (int i=0; i < numberOfRequestedResults; i++) {
            result.add(maxID);
        }
        return result;
    }


    public static List<Long> predict_fallback_old(Item item, Long domainID, int numberOfRequestedResults) {
        log.debug("used fallback prediction for domain "+domainID);
        List<Long> result = new ArrayList<>();
        if (item == null) return result;
        if (item.getItemID() == null) return result;

        Long maxID = item.getItemID();
        for (int i=0; i<numberOfRequestedResults; i++) {
            result.add(maxID - i - 1);
        }
        return result;
    }
}
