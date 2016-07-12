package de.dailab.newsreel.recommender.metarecommender.util;

import de.dailab.newsreel.recommender.common.util.SelfSortingTreeSet;
import de.dailab.newsreel.recommender.metarecommender.rank.RecommenderRank;
import org.junit.Before;
import org.junit.Test;

import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by domann on 22.12.15.
 */
public class SelfSortingTreeSetTest {

    private SelfSortingTreeSet<RecommenderRank> ranking = new SelfSortingTreeSet<>();

    @Before
    public void setUp() throws Exception {
        ranking.add(new RecommenderRank("CF", 12));
        ranking.add(new RecommenderRank("CBF", 10));
        ranking.add(new RecommenderRank("MP", 20));
    }

    @Test
    public void whenListIsInitialized_thenSortOrderIsLikeExpected() throws Exception {
        assertTrue(isSortOrderOk(ranking));
        assertEquals(ranking.first().getRecommenderName(), "MP");
        assertEquals(ranking.last().getRecommenderName(), "CBF");
    }

    @Test
    public void whenElementIsModified_thenSortOrderIsLikeExpected() throws Exception {
        ranking.last().setUtil(25);
        assertEquals(ranking.first().getRecommenderName(), "CBF");
        assertTrue(isSortOrderOk(ranking));
    }

    @Test
    public void whenElementIsModifiedMultipleTimes_thenSortOrderIsLikeExpected() throws Exception {
        RecommenderRank last = ranking.last();
        for (int i = 0; i < 3; i++) {
            last.increaseUtil();
        }
        // sort order is descending (ascending is default) this means higher == lower (brain fuck!!!)
        assertEquals(ranking.higher(ranking.first()).getRecommenderName(), "CBF");
    }

    @Test
    public void whenNewElementIsAdded_thenSortOrderIsLikeExpected() throws Exception {
        assertTrue(isSortOrderOk(ranking));
        ranking.add(new RecommenderRank("CF'", 18));
        assertTrue(isSortOrderOk(ranking));
    }

    private boolean isSortOrderOk(SortedSet<RecommenderRank> ranking) {
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
