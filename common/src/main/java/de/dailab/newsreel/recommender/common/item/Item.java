package de.dailab.newsreel.recommender.common.item;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by jens on 08.12.15.
 */
public interface Item extends Serializable {

    Long getUserID();

    Long getItemID();

    Long getTimestamp();

    ArrayList<Long> getCategoryIds();

}
