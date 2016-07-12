package de.dailab.newsreel.recommender.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.dailab.newsreel.recommender.common.enums.Simple;
import de.dailab.newsreel.recommender.common.extimpl.PlistaItem;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by domann on 20.01.16.
 */
public class ItemUtils {

    private static final Logger log = Logger.getLogger(ItemUtils.class);

    public static PlistaItem parsePlistaItem(ObjectMapper mapper, final String typeValue, final String propertyValue)
            throws IOException {
        JsonNode node = ItemUtils.createJsonNode(mapper, propertyValue);
        switch (typeValue) {
            case PlistaItem.TYPE_RECOMMENDATION_REQUEST:
                return parseRecommendationRequest(node,typeValue, propertyValue);
            case PlistaItem.TYPE_EVENT_NOTIFICATION:
                return parseEventNotification(node, typeValue, propertyValue);
            default:
                return null;
        }
    }

    private static JsonNode createJsonNode(ObjectMapper mapper, String propertyValue) throws IOException {
        return mapper.readTree(propertyValue);
    }

    private static PlistaItem parseRecommendationRequest(JsonNode node, String typeValue, String propertyValue) {
        // recommendation has no timestamp!

        Long itemID = ItemUtils.parseItemId(node);
        if(itemID == null) {
            log.error("itemId is null for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        Long userID = ItemUtils.parseUserId(node);
        if(userID == null) {
            log.error("userID is null for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        Long domainID = ItemUtils.parseDomainId(node);
        if(domainID == null) {
            log.error("domainID is null for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        Long timestamp = System.currentTimeMillis();
        Integer numberOfPredictions = ItemUtils.parseNumberOfPredictions(node);
        if(numberOfPredictions == null) {
            log.error("numberOfPredictions is null for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        ArrayList<Long> categories = ItemUtils.parseCategoriesId(node);
        if(categories == null || categories.size() == 0) {
            log.error("categories is null or empty for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        return new PlistaItem(itemID, userID, domainID, timestamp, numberOfPredictions, null, categories);
    }

    private static PlistaItem parseEventNotification(JsonNode node, String typeValue, String propertyValue) {

        String impressionType = node.at("/type").asText();
        if(impressionType == null) {
            log.error("impressionType is null for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        return parseImpression(node, impressionType, typeValue, propertyValue);

    }

    private static PlistaItem parseImpression(JsonNode node, String impressionType,
                                              String typeValue, String propertyValue)  {
        // event notification has no limit!

        Long itemID = ItemUtils.parseItemId(node);
        if(itemID == null) {
            log.error("itemId is null for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        Long userID = ItemUtils.parseUserId(node);
        if(userID == null) {
            log.error("userID is null for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        Long domainID = ItemUtils.parseDomainId(node);
        if(domainID == null) {
            log.error("domainID is null for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        Long timestamp = ItemUtils.parseTimeStamp(node);
        if(timestamp == null) {
            log.error("timestamp is null for request: "
                    .concat(typeValue).concat(" ").concat(propertyValue));
        }
        ArrayList<Long> categories = ItemUtils.parseCategoriesId(node);
        /*if(categories == null || categories.size() == 0) {
            log.error("categories is null or empty");
        }*/
        return new PlistaItem(itemID, userID, domainID, timestamp, Integer.MIN_VALUE, impressionType, categories);
    }

    public static Long parseItemId(JsonNode node) {
        return parseSimpleLong(node, Simple.ITEM_ID);
    }

    public static Long parseUserId(JsonNode node) {
         return parseSimpleLong(node, Simple.USER_ID);
    }

    private static Long parseDomainId(JsonNode node) {
         return parseSimpleLong(node, Simple.DOMAIN_ID);
    }

    private static ArrayList<Long> parseCategoriesId(JsonNode node) {
        return parseSimpleList(node, Simple.CATEGORY);
    }

    private static Long parseTimeStamp(JsonNode node) {
        Long timeStamp = null;
        if( (timeStamp = parseSimpleLong(node, "/created_at")) == null) {
            timeStamp = parseSimpleLong(node, "/timestamp");
        }
        if (timeStamp == null) {
            return System.currentTimeMillis();
        } else {
            return timeStamp;
        }
    }

    private static Integer parseNumberOfPredictions(JsonNode node) {
        return parseSimpleInteger(node, "/limit");
    }

    private static Long parseSimpleLong(JsonNode node, Simple simple) {
        return parseSimpleLong(node, simple.jacksonPath());
    }

    private static ArrayList<Long> parseSimpleList(JsonNode node, Simple simple) {
        return parseSimpleList(node, simple.jacksonPath());
    }

    private static Long parseSimpleLong(JsonNode node, String jacksonPath) {
        JsonNode longNode = node.at(jacksonPath);
        if(longNode.isMissingNode() || !longNode.canConvertToLong()) {
            return null;
        } else {
            return longNode.asLong();
        }
    }

    private static Integer parseSimpleInteger(JsonNode node, String jacksonPath) {
        JsonNode longNode = node.at(jacksonPath);
        if(longNode.isMissingNode() || !longNode.canConvertToInt()) {
            return null;
        } else {
            return longNode.asInt();
        }
    }

    private static ArrayList<Long> parseSimpleList(JsonNode node, String jacksonPath) {
        JsonNode listNode = node.at(jacksonPath);
        if(listNode.isMissingNode()) {
            return null;
        } else {
            ArrayList<Long> result = new ArrayList<>();
            if (listNode.isArray()) {
                for (JsonNode category : listNode) {
                    try {
                        if (!category.isMissingNode() && category.canConvertToLong()) {
                            result.add(category.asLong());
                        }
                    } catch (Exception e) {
                        log.warn("failed to parse category entry " + category);
                    }
                }
            } else {
                if (listNode.canConvertToLong()) {
                    result.add(listNode.asLong());
                } else return null;
            }
            return result;
        }
    }

}
