package de.dailab.newsreel.recommender.metarecommender.rank;

import de.dailab.newsreel.recommender.common.recommender.Recommender;
import de.dailab.newsreel.recommender.common.util.SelfSortingTreeSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by domann on 16.01.16.
 */
public class PredictionRankingTable implements Serializable {

    SelfSortingTreeSet<RecommenderRank> ranking;
    boolean initial;

    public PredictionRankingTable(List<Recommender> recommenders) {
        ranking = new SelfSortingTreeSet<>();
        for (Recommender recommender : recommenders) {
            ranking.add(new RecommenderRank(recommender.getName(), 0));
        }
        this.initial = true;
    }

    public List<Long> getRankedPredictions(int numberOfPredictions, Map<String,
            List<Long>> totalPredictions) {

        if(initial){
            return roundRobin(numberOfPredictions, totalPredictions);
        } else {
            return getByRanking(numberOfPredictions, totalPredictions);
        }

    }

    private List<Long> getByRanking(int numberOfPredictions, Map<String, List<Long>> totalPredictions) {
        List<Long> resultList = new ArrayList<>();
        long totalUtil = calcTotalUtilization(ranking);

        for (RecommenderRank rank : ranking) {
            String recommenderName = rank.getRecommenderName();
            int amount = calcAmount(numberOfPredictions, totalUtil, rank);
            List<Long> prediction = totalPredictions.get(recommenderName);
            for (int i = 0; i < amount ; i++) {
                resultList.add(prediction.get(i));
            }
        }

        return resultList;
    }

    private long calcTotalUtilization(SelfSortingTreeSet<RecommenderRank> ranking) {
        long result = 0;
        for (RecommenderRank rank : ranking) {
            result += rank.getUtil();
        }
        return result;
    }

    private int calcAmount(int numberOfPredictions, long totalUtil, RecommenderRank rank) {
        double util = rank.getUtil();
        double tutil = totalUtil;
        return (int) (tutil / util * numberOfPredictions);
    }

    private List<Long> roundRobin(int numberOfPredictions, Map<String, List<Long>> totalPredictions) {
        int steps = 0;
        List<Long> resultList = new ArrayList<>();
        Iterator iterator = ranking.iterator();
        do {
            if(iterator.hasNext()) {
                String recommenderName = ( (RecommenderRank)iterator.next() ).getRecommenderName();
                List<Long> predictions = totalPredictions.get(recommenderName);
                if(predictions != null) {
                    Long prediction = predictions.get(0);
                    if (prediction instanceof Long) {
                        resultList.add(prediction);
                        predictions.remove(0);
                    }
                }
            } else {
                iterator = ranking.iterator();
            }
            steps++;
        } while (steps < numberOfPredictions);
        return resultList;
    }

    public void performUtilization(List<String> recommenderNames) {
        for(String name : recommenderNames) {
            performUtilization(name);
        }
    }

    public void performUtilization(String recommenderName) {
        for (RecommenderRank rank : ranking) {
            if(rank.getRecommenderName().equals(recommenderName)) {
                rank.increaseUtil();
            }
        }
    }

}
