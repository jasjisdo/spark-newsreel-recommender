//package de.dailab.newsreel.recommender.metarecommender.util;
//
//import de.dailab.newsreel.recommender.common.util.UpdateableTreeSet;
//import de.dailab.newsreel.recommender.metarecommender.rank.RecommenderRank;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.SortedSet;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
///**
// * Created by domann on 21.12.15.
// */
//public class UpdateableTreeSetTest {
//
//    private static UpdateableTreeSet<RecommenderRank> ranking;
//
//    @Before
//    public void setUp() throws Exception {
//        ranking = new UpdateableTreeSet<RecommenderRank>();
//        ranking.add(new RecommenderRank("CF", 10));
//        ranking.add(new RecommenderRank("IBF", 5));
//        ranking.add(new RecommenderRank("MP", 13));
//    }
//
//    @Test
//    public void nameUnique() {
//        assertEquals(3, ranking.size());
//        ranking.add(new RecommenderRank("CF", 9));
//        assertEquals(3, ranking.size());
//    }
//
//    @Test
//    public void initialRanking() {
//        assertEquals("MP", ranking.first().getRecommenderName());
//        assertEquals("IBF", ranking.last().getRecommenderName());
//    }
//
//    private boolean isSortOrderOk(SortedSet<RecommenderRank> ranking) {
//        // Check total sort order by comparing pairs of neighbours
//        RecommenderRank previousMedalCount = null;
//        for (RecommenderRank currentMedalCount : ranking) {
//            if (previousMedalCount != null && previousMedalCount.compareTo(currentMedalCount) > 0)
//                return false;
//            previousMedalCount = currentMedalCount;
//        }
//        return true;
//    }
//
//    @Test
//    public void bulkUpdate() {
//
//    }
//
//}
