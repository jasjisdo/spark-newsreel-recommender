package de.dailab.newsreel.recommender.common.extimpl;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import de.dailab.newsreel.recommender.common.enums.Entity;
import de.dailab.newsreel.recommender.common.enums.Simple;
import de.dailab.newsreel.recommender.common.inter.Item;

import java.util.ArrayList;

/**
 * Created by domann on 18.12.15.
 */
public class IdomaarItem implements Item{

    protected Long itemId;
    protected Long userId;
    protected Long domainId;
    protected Long timestamp;

    public IdomaarItem(String propertyValue, String entityValue) throws IllegalArgumentException {

        // TODO handle if some of the parameter is invalid. null or "".

        if(isValid(propertyValue, entityValue)) {

            // parse needed fields. (use entity for better performance!)
            Object o = JsonPath.read(entityValue, Entity.ITEM_ID.jasonPath());
            this.itemId = parseJsonValue(o).longValue();
            o = JsonPath.read(entityValue, Entity.USER_ID.jasonPath());
            this.userId = parseJsonValue(o).longValue();
            this.domainId = new Long((int)JsonPath.read(entityValue, Entity.DOMAIN_ID.jasonPath()));
            this.timestamp = System.currentTimeMillis();

        } else {
            throw new IllegalArgumentException("missing required json statment.");
        }
    }

    private boolean isValid(String propertyValue, String entityValue) {

        // parse user ids.
        Object o = JsonPath.read(propertyValue, Simple.USER_ID.jasonPath());
        Integer propertyUserId = parseJsonValue(o);

         o = JsonPath.read(entityValue, Entity.USER_ID.jasonPath());
        Integer entityUserId = parseJsonValue(o);

        // parse item ids.
        try {
            o = JsonPath.read(propertyValue, Simple.ITEM_ID.jasonPath());
        } catch (PathNotFoundException e) {
            o = new Integer(0);
        }
        Integer propertyItemId = parseJsonValue(o);
        o = JsonPath.read(entityValue, Entity.ITEM_ID.jasonPath());
        Integer entityItemId = parseJsonValue(o);

        // parse domain ids.
        Long propertyDomainId = new Long((int)JsonPath.read(propertyValue, Simple.DOMAIN_ID.jasonPath()));
        Long entityDomainId = new Long((int)JsonPath.read(entityValue, Entity.DOMAIN_ID.jasonPath()));

        // compare all parsed values.
        return propertyUserId.equals(entityUserId) &&
                propertyItemId.equals(entityItemId) &&
                propertyDomainId.equals(entityDomainId);

    }

    @Override
    public Long getUserID() {
        return userId;
    }

    @Override
    public Long getItemID() {
        return itemId;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public ArrayList<Long> getCategoryIds() {
        return null;
    }

    public Long getDomainId() {
        return domainId;
    }

    public Integer parseJsonValue(Object o) {
        if(o instanceof Long) {
            return ((Long) o).intValue();
        } else {
            return ((Integer) o);
        }
    }

}
