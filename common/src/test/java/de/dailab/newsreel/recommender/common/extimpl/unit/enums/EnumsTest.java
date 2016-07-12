package de.dailab.newsreel.recommender.common.extimpl.unit.enums;

import de.dailab.newsreel.recommender.common.enums.PlistaType;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by domann on 21.01.16.
 */
public class EnumsTest {

    private static final String ITEM_UPDATE = "item_update";
    private static final String RECOMMENDATION_REQUEST = "recommendation_request";
    public static final String EVENT_NOTIFICATION = "event_notification";
    public static final String ERROR_NOTIFICATION = "error_notification";

    @Test
    public void testName() throws Exception {
        assertThat(PlistaType.valueOf(RECOMMENDATION_REQUEST.toUpperCase()), is(PlistaType.RECOMMENDATION_REQUEST));
        assertThat(PlistaType.valueOf(ITEM_UPDATE.toUpperCase()), is(PlistaType.ITEM_UPDATE));
        assertThat(PlistaType.valueOf(EVENT_NOTIFICATION.toUpperCase()), is(PlistaType.EVENT_NOTIFICATION));
        assertThat(PlistaType.valueOf(ERROR_NOTIFICATION.toUpperCase()), is(PlistaType.ERROR_NOTIFICATION));
    }
}
