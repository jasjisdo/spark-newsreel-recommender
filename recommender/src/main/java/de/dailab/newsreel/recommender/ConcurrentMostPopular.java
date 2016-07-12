package de.dailab.newsreel.recommender;

import de.dailab.newsreel.recommender.common.item.Item;
import de.dailab.newsreel.recommender.common.recommender.Recommender;
import de.dailab.newsreel.recommender.util.ConcurrentSparkList;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * EXPERIMENTAL
 *
 * A item-based most-popular recommender considering time constraints.
 * 
 * @author domann
 *
 */
@Deprecated
public class ConcurrentMostPopular implements Recommender {

    private final Map<Long, ConcurrentSparkList> domainMap;
    private final Set<Long> localBlackList;

    public ConcurrentMostPopular() {

        domainMap = new ConcurrentHashMap<>();

        // initialize the spark lists
        ConcurrentSparkList r418 = new ConcurrentSparkList(30);
        domainMap.put(418L, r418);

        ConcurrentSparkList r596 = new ConcurrentSparkList(30);
        domainMap.put(596L, r596);

        ConcurrentSparkList r694 = new ConcurrentSparkList(30);
        domainMap.put(694L, r694);

        ConcurrentSparkList r1677 = new ConcurrentSparkList(30);
        domainMap.put(1677L, r1677);

        ConcurrentSparkList r35774 = new ConcurrentSparkList(30);
        domainMap.put(35774L, r35774);

        ConcurrentSparkList r13554 = new ConcurrentSparkList(30);
        domainMap.put(13554L, r13554);

        //CompetitionRecommenderWithAdaptationP6 rDefault = new CompetitionRecommenderWithAdaptationP6();
        //rDefault.initializeDefaults(Long.valueOf(defaultDomainID));
        //recommenderByDomain.put(defaultDomainID, rDefault);

        localBlackList = new HashSet<Long>(10);
        localBlackList.add(0L);
        localBlackList.add(-1L);
    }

    @Override
    public String getName() {
        return ConcurrentMostPopular.class.getSimpleName();
    }

    /**
	 * Add a new item to the set of items
	 * @param item the item to add.
	 * @param domainID the domain id of the item.
	 */
	public void update(Item item, Long domainID) {
        if(item != null && item.getItemID() != null && domainID != null) {
            ConcurrentSparkList mySparkList = domainMap.get(domainID);
            if(mySparkList != null) {
                mySparkList.incrementKeyFrequency(item.getItemID());
            } else {
                ConcurrentSparkList sparkList = new ConcurrentSparkList(30);
                domainMap.put(domainID, sparkList);
                sparkList.incrementKeyFrequency(item.getItemID());
            }
        }
	}
	
	/**
	 * Return the best items.
	 * @param item an item
	 * @param domainID an domainID
	 * @param numberOfRequestedResults the expected number of results
	 * @return a list of best items
	 */
	@Override
	public List<Long> predict(Item item, Long domainID, Integer numberOfRequestedResults) {
        if(item.getItemID() == null) return null;

        // create a result list
        final List<Long> resultList = new ArrayList<Long>();

        if((domainID == null)) {
            // fill the missing places with self as fallback
            if (resultList.size() < numberOfRequestedResults) {
                resultList.add(item.getItemID());
            }
            return resultList;
        }

		// create a local blackList
		localBlackList.add(item.getItemID());

        ConcurrentSparkList mySparkList = domainMap.get(domainID);
        if(mySparkList != null) {
            List<Long> delivered = mySparkList.deliver();
            for (int i = 0; i < numberOfRequestedResults; i++) {
                if (i < delivered.size()) {
                    Long key = delivered.get(i);
                    if (!localBlackList.contains(key)) {
                        localBlackList.add(key);
                        resultList.add(key);
                    }
                    if (resultList.size() >= numberOfRequestedResults) {
                        break;
                    }
                }
            }
        }

        // restore local blacklist.
        localBlackList.remove(item.getItemID());

        while (resultList.size() < numberOfRequestedResults) {
		    // fill the missing places with self
			resultList.add(item.getItemID());
		}

		// remove the not requested items
		if (resultList.size() > numberOfRequestedResults) {
			resultList.subList(0, numberOfRequestedResults);
		}

		return resultList;

	}

}
