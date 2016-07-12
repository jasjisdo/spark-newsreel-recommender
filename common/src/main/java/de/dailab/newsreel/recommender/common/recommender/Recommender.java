package de.dailab.newsreel.recommender.common.recommender;

import de.dailab.newsreel.recommender.common.item.Item;

import java.io.Serializable;
import java.util.List;

/**
 *
 *
 * Note: the implementation must provide an emtpy constructor!
 */
public interface Recommender extends Serializable {

    /**
     * This represents a unique human readable identifier for the recommender.
     * @return
     *      unique name as identifier.
     */
    String getName();

    /**
     * handles new impression.
     *
     * @param item
     *
     * @param domainID
     */
    void update(Item item, Long domainID);

    /**
     *
     *
     * @param item
     * @param domainID
     * @param numberOfRequestedResults
     * @return
     */
    List<Long> predict(Item item, Long domainID, Integer numberOfRequestedResults);

}
