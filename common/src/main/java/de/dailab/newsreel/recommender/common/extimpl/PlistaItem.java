package de.dailab.newsreel.recommender.common.extimpl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.dailab.newsreel.recommender.common.enums.Simple;
import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.common.util.ItemUtils;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Integer itemID = JsonPath.read(jsonMessage, "$.context.simple.25");
 * Long domainID  = JsonPath.read(jsonMessage, "$.context.simple.27");
 * Integer userID = JsonPath.read(jsonMessage, "$.context.simple.57");
 * <p/>
 * Created by domann on 11.12.15.
 */
public class PlistaItem implements Item {

    public static final String TYPE_ITEM_UPDATE = "item_update";
    public static final String TYPE_RECOMMENDATION_REQUEST = "recommendation_request";
    public static final String TYPE_EVENT_NOTIFICATION = "event_notification";
    public static final String TYPE_ERROR_NOTIFICATION = "error_notification";

    public static final String EVENT_IMPRESSION = "impression";
    public static final String EVENT_IMPRESSION_EMPTY = "impression_empty";
    public static final String EVENT_CLICK = "click";


    private static ObjectMapper mapper = new ObjectMapper();;

    protected Long itemId;
    protected Long userId;
    protected Long domainId;
    protected Long timestamp;
    protected ArrayList<Long> categories;
    protected Integer numberOfPredictions;

    protected final String impressionType; // null if item is not a impression.

    public PlistaItem(Long itemId, Long userId, Long domainId, Long timestamp, Integer numberOfPredictions,
                      String impressionType, ArrayList<Long> categories) {
        this.itemId = itemId;
        this.userId = userId;
        this.domainId = domainId;
        this.timestamp = timestamp;
        this.numberOfPredictions = numberOfPredictions;
        this.impressionType = impressionType;
        this.categories = categories;
    }

    public static PlistaItem parsePlistaRequest(final String typeValue, final String propertyValue) {
        PlistaItem item = null;
        try {
            item = ItemUtils.parsePlistaItem(mapper, typeValue, propertyValue);
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return item;
    }

    @Override
    public Long getItemID() {
        return itemId;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public ArrayList<Long> getCategoryIds() {
        return categories;
    }

    @Override
    public Long getUserID() {
        return userId;
    }

    public Long getDomainId() {
        return domainId;
    }

    public Integer getNumberOfPredictions() {
        return numberOfPredictions;
    }

    public String getImpressionType() {
        return impressionType;
    }
}
