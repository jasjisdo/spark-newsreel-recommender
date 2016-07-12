package de.dailab.newsreel.recommender.common.extimpl.unit.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import de.dailab.newsreel.recommender.common.util.UpdateableTreeSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UpdateableTreeSetTest
{
	private static UpdateableTreeSet<MedalCount> medalRanking;

	@Before
	public void setUp() throws Exception {
		medalRanking = new UpdateableTreeSet<MedalCount>();
		medalRanking.add(new MedalCount("GER", 1,  2,  3));
		medalRanking.add(new MedalCount("SUI", 1,  2,  3));
		medalRanking.add(new MedalCount("USA", 9,  8,  7));
	}

	@After
	public void tearDown() throws Exception {
		medalRanking = null;
	}

	@Test
	public void countryUnique() {
		assertEquals(3, medalRanking.size());
		medalRanking.add(new MedalCount("GER", 4,  5,  6));
		assertEquals(3, medalRanking.size());
	}

	@Test
	public void initialRanking() {
		// USA has the most medals
		assertEquals("USA", medalRanking.first().country);
		// SUI's medal count is identical to GER's, but SUI is behind GER alphabetically
		assertEquals("SUI", medalRanking.last().country);
	}

	@Test
	public void countryBasedEquality() {
		assertTrue(medalRanking.contains(new MedalCount("GER")));
		assertTrue(medalRanking.contains(new MedalCount("USA", 5, 5, 5)));
		assertFalse(medalRanking.contains(new MedalCount("FRA", 1, 2, 3)));
	}

	@Test
	public void bogusRankingOnDirectPropertyChange() {
		// Sort order should be OK before messing with properties
		assertTrue(isSortOrderOk(medalRanking));

		// Changed properties do not affect existing sort order :-(
		medalRanking.first().gold = 0;
		assertEquals("USA", medalRanking.first().country);
		medalRanking.last().country = "AAA";
		assertEquals("AAA", medalRanking.last().country);

		// Adding a new element to a falsely sorted TreeSet makes things even worse
		medalRanking.add(new MedalCount("AUT", 1,  2,  3));
		// This is the expected (wrong) result
		assertEquals("AUT", medalRanking.first().country);

		// Sort order is wrong now for multiple reasons
		assertFalse(isSortOrderOk(medalRanking));
	}

	@Test
	public void cannotFindElementAfterDirectPropertyChange() {
		// Sort order should be OK before messing with properties
		assertTrue(isSortOrderOk(medalRanking));

		// Naively a user could try this:
		//   1) get element reference
		MedalCount manuallyChangedElement = medalRanking.first();
		//   2) change property
		manuallyChangedElement.gold = 0;
		//   3) tell UpdateableTreeSet to update (i.e. remove + add back) element within set
		boolean updateResult = medalRanking.update(manuallyChangedElement);
		// But the problem with this approach is: It does not work because the element to be updated cannot be found
		// in the TreeSet anymore after one of its key properties has changed. It needs to be removed from the set
		// *before* changing any of its key properties.
		//
		// Lesson learnt: Don't do this at home, kids! Use deferred updates as shown further below.
		assertFalse(updateResult);

		// Just to prove my point: The order within the SortedSet is broken.
		assertFalse(isSortOrderOk(medalRanking));
	}

	@Test
	public void updateAllAfterDirectPropertyChange() {
		// Sort order should be OK before messing with properties
		assertTrue(isSortOrderOk(medalRanking));

		// Mark first element for removal (should not happen because updateAll should reset all marks)
		medalRanking.markForRemoval(medalRanking.first());
		// Wildly change element properties and add a new element
		medalRanking.first().gold = 0;
		medalRanking.last().country = "AAA";
		medalRanking.add(new MedalCount("AUT", 1,  2,  3));
		// Sort order is now broken
		assertFalse(isSortOrderOk(medalRanking));

		// Repair set by totally re-sorting it
		medalRanking.updateAll();

		// Sort order should now be OK again
		assertEquals("AAA", medalRanking.first().country);
		assertEquals("USA", medalRanking.last().country);
		assertTrue(isSortOrderOk(medalRanking));
		// Number of elements should still be 4 because remove marks should have been reset by updateAll
		assertEquals(4, medalRanking.size());
	}

	@Test
	public void updateSingleElements() {
		// Sort order should be OK before messing with properties
		assertTrue(isSortOrderOk(medalRanking));

		// Deferred property update via key/value map should be OK
		Map<String, Object> newValues = new HashMap<String, Object>();
		newValues.put("gold", 0);
		medalRanking.update(medalRanking.first(), newValues);
		// Now USA should be on last rank (0 gold medals), GER on first
		assertEquals("GER", medalRanking.first().country);
		assertTrue(isSortOrderOk(medalRanking));

		// Deferred property update via key/value map should be OK
		newValues.clear();
		newValues.put("country", "ZYX");
		medalRanking.update(medalRanking.first(), newValues);
		// Renaming GER to ZYX should put SUI on first rank (alphabetical order kicks in upon equal medal stats)
		assertEquals("SUI", medalRanking.first().country);
		assertTrue(isSortOrderOk(medalRanking));

		// Adding a new element to a correctly updated TreeSet should be OK
		medalRanking.add(new MedalCount("FRA", 2,  0,  0));
		assertEquals("FRA", medalRanking.first().country);
		assertTrue(isSortOrderOk(medalRanking));
	}

	@Test
	public void bulkUpdate() {
		// Turn all property values upside-down + mark one entry for removal
		for (MedalCount medalCount : medalRanking) {
			if ("GER".equals(medalCount.country)) {
				medalRanking.markForRemoval(medalCount);
				continue;
			}
			Map<String, Object> newValues = new HashMap<String, Object>();
			// SUI -> FHV, USA -> HFN
			newValues.put("country", rot13(medalCount.country));
			newValues.put("gold",   10 - medalCount.gold);
			newValues.put("silver", 10 - medalCount.silver);
			newValues.put("bronze", 10 - medalCount.bronze);
			medalRanking.markForUpdate(medalCount, newValues);
		}

		// Perform bulk update
		medalRanking.updateMarked();

		// Check if some of the changed properties are as expected
		assertEquals("FHV", medalRanking.first().country);
		assertEquals(7, medalRanking.first().bronze);
		assertEquals("HFN", medalRanking.last().country);
		assertEquals(2, medalRanking.last().silver);

		// Add more entries and also check their positions
		medalRanking.add(new MedalCount("FRA", 1,  2,  0));
		assertEquals("FRA", medalRanking.last().country);
		medalRanking.add(new MedalCount("RUS", 9,  9,  4));
		assertEquals("RUS", medalRanking.first().country);

		// Check overall sort order
		assertTrue(isSortOrderOk(medalRanking));
	}

	@Test
	public void addElementsDuringLoop() {
		for (MedalCount medalCount : medalRanking) {
			medalRanking.markForUpdate(
				new MedalCount(
						medalCount.country + "X",
						medalCount.gold + 1,
						medalCount.silver + 2,
						medalCount.bronze + 3
				)
			);
		}

		// Perform bulk update
		medalRanking.updateMarked();

		// Number of elements should have doubled to 6 now
		assertEquals(6, medalRanking.size());

		// Check if some of the changed properties are as expected
		assertEquals("USAX", medalRanking.first().country);
		assertEquals(10, medalRanking.first().bronze);

		// Check overall sort order
		assertTrue(isSortOrderOk(medalRanking));
	}

	@Test
	public void noValueBulkUpdate() {
		// Verify that no-op update does not do any damage
		medalRanking.markForUpdate(medalRanking.first());
		medalRanking.markForUpdate(medalRanking.last());
		medalRanking.updateMarked();
		assertTrue(isSortOrderOk(medalRanking));
	}

	@Test @SuppressWarnings({ "rawtypes", "unchecked" })
	public void parametrisedConstructors() {
		UpdateableTreeSet<MedalCount> medalRankingCopy;
		List medalRankingList;

		// Create an UpdateableTreeSet from another one
		medalRankingCopy = new UpdateableTreeSet<MedalCount>(medalRanking);
		assertEquals(medalRanking.first().country, medalRankingCopy.first().country);
		assertEquals(medalRanking.last().country, medalRankingCopy.last().country);
		assertTrue(isSortOrderOk(medalRankingCopy));

		// Create an UpdateableTreeSet from a Collection
		medalRankingList = Arrays.asList(medalRanking.toArray());
		medalRankingCopy = new UpdateableTreeSet<MedalCount>(medalRankingList);
		assertEquals(medalRanking.first().country, medalRankingCopy.first().country);
		assertEquals(medalRanking.last().country, medalRankingCopy.last().country);
		assertTrue(isSortOrderOk(medalRankingCopy));

		// Create an UpdateableTreeSet with a Comparator
		medalRankingCopy = new UpdateableTreeSet<MedalCount>(
			new Comparator<MedalCount>() {
				@Override
				public int compare(MedalCount mc1, MedalCount mc2) {
					// Compare by country name only, *not* by medal stats
					return mc1.country.compareTo(mc2.country);
				}
			}
		);
		// Add elements
		medalRankingCopy.add(new MedalCount("GER", 1,  2,  3));
		medalRankingCopy.add(new MedalCount("SUI", 1,  2,  3));
		medalRankingCopy.add(new MedalCount("USA", 9,  8,  7));
		medalRankingCopy.add(new MedalCount("RUS", 1,  2,  3));
		medalRankingCopy.add(new MedalCount("ITA", 1,  2,  3));
		medalRankingCopy.add(new MedalCount("FRA", 9,  8,  7));
		medalRankingList = Arrays.asList(medalRankingCopy.toArray());
		// Verify that order is alphabetical by country name, as implied by the Comparator
		assertEquals("FRA", ((MedalCount) medalRankingList.get(0)).country);
		assertEquals("GER", ((MedalCount) medalRankingList.get(1)).country);
		assertEquals("ITA", ((MedalCount) medalRankingList.get(2)).country);
		assertEquals("RUS", ((MedalCount) medalRankingList.get(3)).country);
		assertEquals("SUI", ((MedalCount) medalRankingList.get(4)).country);
		assertEquals("USA", ((MedalCount) medalRankingList.get(5)).country);
	}

	private boolean isSortOrderOk(SortedSet<MedalCount> medalRanking) {
		// Check total sort order by comparing pairs of neighbours
		MedalCount previousMedalCount = null;
		for (MedalCount currentMedalCount : medalRanking) {
			if (previousMedalCount != null && previousMedalCount.compareTo(currentMedalCount) > 0)
				return false;
			previousMedalCount = currentMedalCount;
		}
		return true;
	}

	private String rot13(String text) {
		char[] rotated = new char[text.length()];
		for (int i = 0; i < text.length(); i++) {
			char c = text.charAt(i);
			if (c >= 'a' && c <= 'm' || c >= 'A' && c <= 'M')
				c += 13;
			else if (c >= 'n' && c <= 'z' || c >= 'N' && c <= 'Z')
				c -= 13;
			rotated[i] = c;
		}
		return new String(rotated);
	}

}
