package de.dailab.newsreel.recommender.metarecommender.util;

import de.dailab.newsreel.recommender.common.util.SelfSortingArrayList;
import de.dailab.newsreel.recommender.metarecommender.rank.RecommenderRank;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by domann on 22.12.15.
 */
public class ObservableArrayListTest {

    private SelfSortingArrayList<RecommenderRank> ranking = new SelfSortingArrayList<RecommenderRank>();

    @Before
    public void setUp() throws Exception {
        ranking.add(new RecommenderRank("CF", 12));
        ranking.add(new RecommenderRank("CBF", 10));
        ranking.add(new RecommenderRank("MP", 20));
    }

    @Test
    public void whenListIsInitialized_thenSortOrderIsLikeExpected() throws Exception {
        assertTrue(isSortOrderOk(ranking));
        assertEquals(ranking.get(0).getRecommenderName(), "MP");
        assertEquals(ranking.get(1).getRecommenderName(), "CF");
        assertEquals(ranking.get(2).getRecommenderName(), "CBF");
    }

    @Test
    public void whenElementIsModified_thenSortOrderIsLikeExpected() throws Exception {
        ranking.get(2).setUtil(15);
        assertEquals(ranking.get(1).getRecommenderName(), "CBF");
        assertTrue(isSortOrderOk(ranking));
    }

    @Test
    public void whenElementIsModifiedMultipleTimes_thenSortOrderIsLikeExpected() throws Exception {
        RecommenderRank last = ranking.get(2);
        for (int i = 0; i < 3; i++) {
            last.increaseUtil();
        }
        assertEquals(ranking.get(1).getRecommenderName(), "CBF");
    }

    @Test
    public void whenNewElementIsAdded_thenSortOrderIsLikeExpected() throws Exception {
        assertTrue(isSortOrderOk(ranking));
        ranking.add(new RecommenderRank("CF'", 18));
        assertTrue(isSortOrderOk(ranking));
    }

    private boolean isSortOrderOk(List<RecommenderRank> ranking) {
        // Check total sort order by comparing pairs of neighbours
        RecommenderRank previousRank = null;
        for (RecommenderRank currentRank : ranking) {
            if (previousRank != null && previousRank.compareTo(currentRank) < 0)
                return false;
            previousRank = currentRank;
        }
        return true;
    }
}
