package de.dailab.newsreel.recommender.metarecommender.requesthandler;

import de.dailab.newsreel.recommender.common.item.IdomaarItem;
import de.dailab.newsreel.recommender.common.item.Item;
import de.dailab.newsreel.recommender.common.recommender.Recommender;
import de.dailab.newsreel.recommender.common.util.SelfSortingTreeSet;
import de.dailab.newsreel.recommender.metarecommender.context.Context;
import de.dailab.newsreel.recommender.metarecommender.delegate.RecommenderDelegate;
import de.dailab.newsreel.recommender.metarecommender.inter.RequestHandler;
import de.dailab.newsreel.recommender.metarecommender.rank.PredictionRankingTable;
import de.dailab.newsreel.recommender.metarecommender.rank.RecommenderRank;
import de.dailab.newsreel.recommender.metarecommender.util.RequestUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.*;

/**
 * Created by domann on 18.12.15.
 */
public class IdomaarRequestHandler implements RequestHandler {

    private static final Logger log = Logger.getLogger(IdomaarRequestHandler.class);

    private static final String IMPRESSION = "impression";
    private static final String RECOMMENDATION = "recommendation";
    private static final String CLICK = "click";
    private static final String ERROR_NOTIFICATION = "error_notification";
    private static final int NUMBER_OF_PREDICTIONS = 6;
    private static final int IMPRESSION_PRINT_INTERVAL = 300;

    private int impression_count = 0;
    final RecommenderDelegate delegate;
    private Map<Context, SelfSortingTreeSet<RecommenderRank>> contextRanking;
    private PredictionRankingTable predictionRankingTable;

    /** Pair<Item, Long> : impression as Item -> prediction as Long (id) */
    private Map<Pair<Item, Long>, List<String>> recommendations;
    private IdomaarItem recommenderItem;

    public IdomaarRequestHandler() {

        // use spring framework to inject recommenders with delegate.
        ApplicationContext appContext = new ClassPathXmlApplicationContext("RecommenderDelegate.xml");
        this.delegate = appContext.getBean(RecommenderDelegate.class);

        if(log.isDebugEnabled()) {
            for (Recommender recommender : delegate.getRecommenders()) {
                log.debug(String.format("recommender: class='%s' name='%s'",
                        recommender.getClass().getSimpleName(), recommender.getName()));
            }
        }

        predictionRankingTable = new PredictionRankingTable(delegate.getRecommenders());

        contextRanking = new Hashtable<>();
        // TODO init context contextRanking here.
        // iterate over contexts
//        for (Context context : delegate.getContexts()) {
//            // create self sorting tree set for every context
//            SelfSortingTreeSet<RecommenderRank> selfSortingTreeSet = new SelfSortingTreeSet<>();
//            // collect recommenders in selfsorting tree set
//            for (Recommender recommender : delegate.getRecommenders()) {
//                selfSortingTreeSet.add(new RecommenderRank(recommender.getName(), 0));
//            }
//            // put all to contextRanking map.
//            contextRanking.put(context, selfSortingTreeSet);
//        }

        recommendations = new HashMap<>();
        // Context context = …
        // contextRanking.put(context, utils);

    }

    @Override
    public String handleRequest(String... params) throws IllegalArgumentException {

        String typeValue = null;
        String propertyValue = null;
        String entityValue = null;

        if (params.length == 3) {
            typeValue = params[0];
            propertyValue = params[1];
            entityValue = params[2];
        } else {
            throw new IllegalArgumentException("wrong parameters for idomaar handler.");
        }

        log.debug(typeValue + "\t" + propertyValue + "\t" + entityValue);

        String response = null;
        recommenderItem = new IdomaarItem(propertyValue, entityValue);
        Long domainId = recommenderItem.getDomainId();

        switch (typeValue.toLowerCase()) {

            case IMPRESSION:
                ++impression_count;
                if ((impression_count % IMPRESSION_PRINT_INTERVAL) == 0) {
                    log.info("impression count = " + impression_count);
                }
                response = RequestUtils.handleUpdate(delegate, recommenderItem, domainId);
                break;

            case RECOMMENDATION:
                Map<String, List<Long>> resultMap = delegate.predict(recommenderItem, domainId, NUMBER_OF_PREDICTIONS);
                List<Long> predictions = predictionRankingTable.getRankedPredictions(NUMBER_OF_PREDICTIONS, resultMap);
                recordRecommendations(recommenderItem, resultMap);
                response = "{\"recs\": {\"ints\": {\"3\": ".concat(predictions.toString()).concat(" } } }");
                break;

            case CLICK:
                List<String> recommenderNames = getRecommenderNameById(recommenderItem);
                if(recommenderNames != null) {
                    predictionRankingTable.performUtilization(recommenderNames);
                }
                response = "handle click eventNotification successful";
                break;

            case ERROR_NOTIFICATION:
                RequestUtils.handleErrorNotification(propertyValue, entityValue);
                break;

            default:
                RequestUtils.handleUnknownMessageType(typeValue, propertyValue, entityValue);
        }

        return response;
    }

    private void recordRecommendations(IdomaarItem recommenderItem, Map<String, List<Long>> resultMap) {
        for (String recommenderName : resultMap.keySet()) {
            for (Long id : resultMap.get(recommenderName)) {
                Pair<Item, Long> pair = new ImmutablePair<>(recommenderItem, id);
                if(recommendations.containsKey(pair)) {
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

    private List<String> getRecommenderNameById(Item item) {
        for(Pair<Item, Long> pair : recommendations.keySet()) {
            if(pair.getValue().equals(item.getItemID())){
                return recommendations.get(pair);
            }
        }
        return null;
    }

}
