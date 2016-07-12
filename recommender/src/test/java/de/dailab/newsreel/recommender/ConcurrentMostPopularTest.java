package de.dailab.newsreel.recommender;

import de.dailab.newsreel.recommender.common.baseimpl.BasicItem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Created by domann on 04.04.16.
 */
public class ConcurrentMostPopularTest {

    private ConcurrentMostPopular concurrentMostPopular;

    @Before
    public void setUp() throws Exception {
        concurrentMostPopular = new ConcurrentMostPopular();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void name() throws Exception {
        concurrentMostPopular.update(new BasicItem(1L, 1L), 418L);
        concurrentMostPopular.update(new BasicItem(2L, 1L), 418L);
        concurrentMostPopular.update(new BasicItem(3L, 1L), 418L);
        concurrentMostPopular.update(new BasicItem(3L, 2L), 418L);
        concurrentMostPopular.update(new BasicItem(1L, 2L), 418L);

        BasicItem item = new BasicItem(3L, 2L);
        List<Long> result = concurrentMostPopular.predict(item, 418L, 3);
        System.out.println(result);
    }
}
