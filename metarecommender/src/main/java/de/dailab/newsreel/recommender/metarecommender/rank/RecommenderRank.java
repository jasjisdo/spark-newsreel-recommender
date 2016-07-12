package de.dailab.newsreel.recommender.metarecommender.rank;

import de.dailab.newsreel.recommender.common.abstracts.ComparableObservable;

import java.io.Serializable;

/**
 * Created by domann on 21.12.15.
 */
public class RecommenderRank extends ComparableObservable<RecommenderRank> implements Serializable{

    private final String recommenderName;
    private long util;
    private long numberOfSubmittedRecommendations;

    public RecommenderRank(String recommenderName, int util) {
        this.recommenderName = recommenderName;
        this.util = util;
        this.numberOfSubmittedRecommendations = 0;
    }

    public String getRecommenderName() {
        return recommenderName;
    }

    public long getUtil() {
        return util;
    }

    public void increaseUtil(){
        util++;
        this.setChanged();
        this.notifyObservers(util);
    }

    public void setUtil(long util) {
        this.util = util;
        this.setChanged();
        this.notifyObservers(util);
    }

    public long getNumberOfSubmittedRecommendations() {
        return numberOfSubmittedRecommendations;
    }

    public void inreaseSubmittedRecommendations(long number) {
        this.numberOfSubmittedRecommendations += number;
    }

    public void inreaseSubmittedRecommendation() {
        numberOfSubmittedRecommendations++;
    }

    @Override
    public int hashCode() {
        return recommenderName.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecommenderRank)) return false;

        RecommenderRank that = (RecommenderRank) o;

        return recommenderName.equals(that.recommenderName);

    }

    @Override
    public int compareTo(RecommenderRank o) {
        if (this.equals(o)) {
            return 0;
        }

        if(this.util < o.util) {
            return -1;
        } else if(this.util > o.util) {
            return 1;
        }
        return recommenderName.compareTo(o.recommenderName);
    }

}
