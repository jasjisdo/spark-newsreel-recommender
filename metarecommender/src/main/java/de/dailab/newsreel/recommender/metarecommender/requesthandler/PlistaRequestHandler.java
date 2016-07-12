package de.dailab.newsreel.recommender.metarecommender.requesthandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.UnsignedLong;
import de.dailab.newsreel.recommender.common.defaults.PlistaParameter;
import de.dailab.newsreel.recommender.common.extimpl.PlistaItem;
import de.dailab.newsreel.recommender.common.inter.Item;
import de.dailab.newsreel.recommender.common.inter.Recommender;
import de.dailab.newsreel.recommender.metarecommender.context.Context;
import de.dailab.newsreel.recommender.metarecommender.delegate.RecommenderDelegate;
import de.dailab.newsreel.recommender.metarecommender.inter.RequestHandler;
import de.dailab.newsreel.recommender.metarecommender.rank.PredictionRankingTable;
import de.dailab.newsreel.recommender.metarecommender.util.RequestUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by domann on 18.12.15.
 */
public class PlistaRequestHandler implements RequestHandler {

    private static final Logger log = Logger.getLogger(PlistaRequestHandler.class);
    private static final BigInteger IMPRESSION_PRINT_INTERVAL = BigInteger.valueOf(100);
    private static final BigInteger RECOMMENDATION_PRINT_INTERVAL = BigInteger.valueOf(500);
    private static final BigInteger REQUEST_PRINT_INTERVAL = BigInteger.valueOf(500);
    private static final String ITEM_IS_INSUFFICIENT_BECAUSE_IT_HAS_NO_ID = "item is insufficient, because it has no id!";

    private BigInteger impression_count     = BigInteger.ZERO;
    private BigInteger recommendation_count = BigInteger.ZERO;
    private BigInteger request_count        = BigInteger.ZERO;

    private final ObjectMapper mapper;
    private final RecommenderDelegate delegate;

    private Map<Context, int[]> ranking;

    /**
     * Pair<Item, Long> : impression as Item -> prediction as Long (id)
     */
    private Map<Pair<Item, Long>, List<String>> recommendations;
    private PlistaItem recommenderItem;
    private PredictionRankingTable predictionRankingTable;

    public PlistaRequestHandler() {

        mapper = new ObjectMapper();

        // use spring framework to inject recommenders with delegate.
        ApplicationContext context = new ClassPathXmlApplicationContext("RecommenderDelegate.xml");
        this.delegate = context.getBean(RecommenderDelegate.class);

        if (log.isDebugEnabled()) {
            for (Recommender recommender : delegate.getRecommenders()) {
                log.debug(String.format("recommender: class='%s' name='%s'",
                        recommender.getClass().getSimpleName(), recommender.getName()));
            }
        }

        predictionRankingTable = new PredictionRankingTable(delegate.getRecommenders());

        ranking = new Hashtable<>();
        // TODO init context ranking here.
        recommendations = new ConcurrentHashMap<>();
        // Context context = …
        // ranking.put(context, utils);

    }

    @Override
    public String handleRequest(String... params) throws IllegalArgumentException {

        String typeValue = null;
        String propertyValue = null;
        if (params.length == 2) {
            typeValue = params[0];
            propertyValue = params[1];
        } else {
            throw new IllegalArgumentException("wrong parameters for plista handler.");
        }
        log.debug("got message: " + typeValue + "\t" + propertyValue);
        countAndLogRequest();

        switch (typeValue.toLowerCase()) {
            case PlistaItem.TYPE_ITEM_UPDATE:
                return ";item_update successful";

            case PlistaItem.TYPE_RECOMMENDATION_REQUEST:
                countAndLogRecommendation();
                return handlePlistaRecommendation(typeValue, propertyValue);

            case PlistaItem.TYPE_EVENT_NOTIFICATION:
                countAndLogImpression();
                return handlePlistaEventNotification(typeValue, propertyValue);

            case PlistaItem.TYPE_ERROR_NOTIFICATION:
                RequestUtils.handleErrorNotification(typeValue, propertyValue);
                return ";handled error notification";

            default:
                RequestUtils.handleUnknownMessageType(typeValue, propertyValue, "");
                return ";got unknown message type";
        }

    }

    private String handlePlistaRecommendation(String typeValue, String propertyValue) {
        recommenderItem = PlistaItem.parsePlistaRequest(typeValue, propertyValue);
        Integer numberOfPredictions = null;

        if (numberOfPredictions != null) {
            numberOfPredictions = recommenderItem.getNumberOfPredictions();
        } else {
            numberOfPredictions = PlistaParameter.DEFAULT_NUMBER_OF_PREDICTIONS;
        }
        Map<String, List<Long>> resultMap = delegate.predict(recommenderItem, recommenderItem.getDomainId(),
                numberOfPredictions);
        List<Long> predictions = predictionRankingTable.getRankedPredictions(
                numberOfPredictions, resultMap);
        recordRecommendations(recommenderItem, resultMap);
        return "{\"recs\": {\"ints\": {\"3\": ".concat(predictions.toString()).concat(" } } }");
    }

    private String handlePlistaEventNotification(String typeValue, String propertyValue) {
        recommenderItem = PlistaItem.parsePlistaRequest(typeValue, propertyValue);
        Long domainId = recommenderItem.getDomainId();

        if (recommenderItem != null
                && recommenderItem.getImpressionType() != null
                && !(recommenderItem.getImpressionType().equals(""))) {
            return handlePlistaImpression(domainId);
        }

        return ITEM_IS_INSUFFICIENT_BECAUSE_IT_HAS_NO_ID;
    }

    /*
     * Count requests and log out.
     */
    private void countAndLogRequest() {
        request_count = request_count.add(BigInteger.ONE);
        if (request_count.mod(REQUEST_PRINT_INTERVAL).equals(UnsignedLong.ZERO)) {
            log.debug("request count = " + request_count);
        }
    }

    /*
     * Count recommendation and log out.
     */
    private void countAndLogRecommendation() {
        recommendation_count = recommendation_count.add(BigInteger.ONE);
        if (recommendation_count.mod(RECOMMENDATION_PRINT_INTERVAL).equals(BigInteger.ONE)) {
            log.debug("recommendation count = " + impression_count);
        }
    }

    /*
     * Count impression and log out.
     */
    private void countAndLogImpression() {
        impression_count = impression_count.add(BigInteger.ONE);
        if (impression_count.mod(IMPRESSION_PRINT_INTERVAL).equals(BigInteger.ZERO.ZERO)) {
            log.debug("impression count = " + impression_count);
        }
    }

    /*
     *
     * @param recommenderItem
     * @param resultMap
     */
    private void recordRecommendations(PlistaItem recommenderItem, Map<String, List<Long>> resultMap) {
        for (String recommenderName : resultMap.keySet()) {
            List<Long> longs = resultMap.get(recommenderName);
            if (longs != null) {
                for (Long id : longs) {
                    if (id != null) {
                        Pair<Item, Long> pair = new ImmutablePair<>(recommenderItem, id);
                        if (recommendations.containsKey(pair)) {
                            List<String> names = recommendations.get(pair);
                            names.add(recommenderName);
                        } else {
                            List<String> names = new ArrayList<>();
                            names.add(recommenderName);
                            recommendations.put(pair, names);
                        }
                    }
                }
            }
        }
    }

    /**
     *
     */
    private String handlePlistaImpression(Long domainId) {
        switch (recommenderItem.getImpressionType().toLowerCase()) {
            case PlistaItem.EVENT_IMPRESSION:
                return RequestUtils.handleUpdate(delegate, recommenderItem, domainId);

            case PlistaItem.EVENT_IMPRESSION_EMPTY:
                return RequestUtils.handleUpdate(delegate, recommenderItem, domainId);

            case PlistaItem.EVENT_CLICK:
                List<String> recommenderNames = getRecommenderNameById(recommenderItem);
                if (recommenderNames != null) {
                    predictionRankingTable.performUtilization(recommenderNames);
                }
                return "handle click eventNotification successful";
        }
        return null;
    }

    /*
     *
     * @param item
     * @return
     */
    private List<String> getRecommenderNameById(Item item) {
        for (Pair<Item, Long> pair : recommendations.keySet()) {
            if (pair.getValue().equals(item.getItemID())) {
                return recommendations.get(pair);
            }
        }
        return null;
    }

}
