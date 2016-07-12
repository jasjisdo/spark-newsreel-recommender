package de.dailab.newsreel.recommender.common.baseimpl;


import de.dailab.newsreel.recommender.common.inter.Item;

import java.util.ArrayList;

/**
 * Created by jens on 08.12.15.
 */
public class BasicItem implements Item {

    private Long user;
    private Long item;
    private Long timestamp;

    public BasicItem(Long userID, Long itemID){
        this.user = userID;
        this.item = itemID;
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public Long getUserID() {
        return user;
    }

    @Override
    public Long getItemID() {
        return item;
    }

    @Override
    public Long getTimestamp() { return timestamp; }

    @Override
    public ArrayList<Long> getCategoryIds() {
        return null;
    }
}
