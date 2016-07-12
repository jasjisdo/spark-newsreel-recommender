package de.dailab.newsreel.recommender.common.extimpl.unit.jsonpath;

import com.jayway.jsonpath.JsonPath;
import de.dailab.newsreel.recommender.common.enums.Entity;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by domann on 18.12.15.
 */
public class jsonPathEntityTest {

    private String entityValue;

    @Before
    public void setUp() throws Exception {
        entityValue = "{\"userID\":71131568, \"itemID\":186312774, \"domainID\":1677}";
    }

    @Test
    public void testName() throws Exception {
        Integer entityUserId = JsonPath.read(entityValue, Entity.USER_ID.jasonPath());
        assertThat(entityUserId, is(71131568));
        Integer entityItemID = JsonPath.read(entityValue, Entity.ITEM_ID.jasonPath());
        assertThat(entityItemID, is(186312774));
        Integer entityDomainID = JsonPath.read(entityValue, Entity.DOMAIN_ID.jasonPath());
        assertThat(entityDomainID, is(1677));
    }
}
