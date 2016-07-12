package de.dailab.newsreel.recommender.metarecommender.util;

import org.junit.Test;
import static de.dailab.newsreel.recommender.metarecommender.util.InputVectorUtils.InputVector.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


/**
 * Created by domann on 07.12.15.
 */
public class InputVectorUtilsTest {

    @Test
    public void whenEnumOrdinalsACalled_thenNoExceptionAndexpectedNumbersAreCorrect() throws Exception {
        assertThat(NULL.ordinal(), is(0));
        assertThat(GENDER.ordinal(), is(1));
        assertThat(AGE.ordinal(), is(2));
        assertThat(INCOME.ordinal(), is(3));
        assertThat(BROWSER.ordinal(), is(4));
    }
}
