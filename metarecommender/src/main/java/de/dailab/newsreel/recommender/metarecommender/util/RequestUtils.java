package de.dailab.newsreel.recommender.metarecommender.util;

import de.dailab.newsreel.recommender.common.extimpl.IdomaarItem;
import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.metarecommender.delegate.RecommenderDelegate;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by domann on 21.12.15.
 */
public class RequestUtils {

    private static final Logger log = Logger.getLogger(RequestUtils.class);
    // static {log.setLevel(org.apache.log4j.Level.DEBUG);}

    /**
     * Create a json response object for recommendation requests.
     *
     * @param itemsIDs          The recommendation result
     * @return                  The recommendation result as json
     */
    public static final String getRecommendationResultJSON(String itemsIDs) {

        // invalid recommendations result in empty result sets
        if (itemsIDs == null || itemsIDs.length() == 0) {
            itemsIDs = "[]";
        }
        // add brackets if needed
        else if (!itemsIDs.trim().startsWith("[")) {
            itemsIDs = "[" + itemsIDs + "]";
        }
        // build result as JSON according to formal requirements
        String result = "{" + "\"recs\": {" + "\"ints\": {" + "\"3\": "
                + itemsIDs + "}" + "}}";

        return result;
    }

    public static void handleUnknownMessageType(String typeValue, String propertyValue, String entityValue) {
        log.error("unknown MessageType: " + typeValue);
        log.error(propertyValue + "\n" + entityValue);
    }

    public static void handleErrorNotification(String propertyValue, String entityValue) {
        log.error("error-notification: " + propertyValue + "\n" + entityValue);
    }

    /**
     * Handles item impression by using delegate.
     *
     * @return      response text.
     */
    public static String handleUpdate(RecommenderDelegate delegate, Item recommenderItem, Long domainId) {
        String response;
        if (recommenderItem != null && recommenderItem.getItemID() != null) {
            // handle update to all recommenders
            delegate.update(recommenderItem, domainId);
        }
        response = ";item_update successfull";
        return response;
    }

}
