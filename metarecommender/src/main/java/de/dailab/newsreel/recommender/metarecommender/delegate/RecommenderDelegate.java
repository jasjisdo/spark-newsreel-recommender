package de.dailab.newsreel.recommender.metarecommender.delegate;

import de.dailab.newsreel.recommender.common.item.Item;
import de.dailab.newsreel.recommender.common.recommender.Recommender;
import de.dailab.newsreel.recommender.metarecommender.context.Context;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by domann on 21.12.15.
 */
public class RecommenderDelegate implements Serializable{

    protected List<Recommender> recommenders;

    protected List<Context> contexts;

    public void update(Item item, Long domainID){
        for (Recommender recommender : recommenders) {
            recommender.update(item, domainID);
        }
    }

    public Map<String, List<Long>> predict (Item item, Long domainID, int numberOfRequestedResults) {
        Map<String, List<Long>> resultMap = new HashMap<>();
        for (Recommender recommender : recommenders) {
            resultMap.put(recommender.getName(), recommender.predict(item, domainID, numberOfRequestedResults));
        }
        return resultMap;
    }

    /**
     * Find a recommender by name in recommender list.
     *
     * @param name      Name of the requiered recommender.
     * @return          A recommender with given name or null if no recommender with given name exists in the list.
     */
    public Recommender getRecommenderByName(String name) {
        for (Recommender recommender : recommenders) {
            if (recommender.getName().equals(name)) {
                return recommender;
            }
        }
        return null;
     }

    public void setRecommenders(List<Recommender> recommenders) {
        this.recommenders = recommenders;
    }

    public List<Recommender> getRecommenders() {
        return recommenders;
    }

    public List<Context> getContexts() {
        return contexts;
    }

    public void setContexts(List<Context> contexts) {
        this.contexts = contexts;
    }
}
