package de.dailab.newsreel.recommender.common.abstracts;

import de.dailab.newsreel.recommender.common.inter.Item;

/**
 * Created by domann on 21.01.16.
 */
public abstract class AbtractPlistaItem implements Item {

    protected Long itemId;
    protected Long userId;
    protected Long domainId;

    public AbtractPlistaItem(Long itemId, Long userId, Long domainId) {
        this.itemId = itemId;
        this.userId = userId;
        this.domainId = domainId;
    }

    public Long getItemId() {
        return itemId;
    }

    public Long getUserId() {
        return userId;
    }

    public Long getDomainId() {
        return domainId;
    }
}
