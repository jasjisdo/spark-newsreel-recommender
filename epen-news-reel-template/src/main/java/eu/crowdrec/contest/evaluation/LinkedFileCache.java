package eu.crowdrec.contest.evaluation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * The class buffers large files for the recommendation quality evaluation.
 * The idea is that we read a certain number of lines in advance.
 * The number of cached lines is define based on a timeSpan parameter.
 * We store the lines in a linked list.
 * Additionally, important data are stored in a HashMap, ensuring access in constant time.
 * 
 * If a new line is requested, we read from the files the all lines relevant for the timeSpan.
 * From all the valid read lines, the important data are stored in the cache. 
 * The oldest line of the cache is removed (from the linkedList and the hash).
 * The oldest line is returned.
 * 
 * In oder to check whether a recommendation is good, we can check in constant time against the hash.
 * 
 * In oder to prevent recommending every time the same item, already viewed items can declared invalid.
 * 
 * Please close the cache in order to release resources.
 * 
 * The class never return the line in the last time window.
 * 
 * Known Problems:
 * - duplicate entries according to the duplicateKey may lead to unexpected behavior.
 * 
 * @author andreas
 *
 */
public class LinkedFileCache {

	/**
	 * A reader for reading the impression file
	 */
	private BufferedReader brImpresssions = null;
	
	/**
	 * The cache for checking what items in user will read in the future
	 */
	private HashMap<String, Boolean> validityByKey = new HashMap<String, Boolean>();
	
	/**
	 * A sorted list of read lines. The first element is the oldest line, the last the newest.
	 */
	private LinkedList<CacheEntry> listLogFileEntries = new LinkedList<CacheEntry>();
	
	/**
	 * marker of end of file reached.
	 */
	private boolean moreLinesToRead = true;
	
	/**
	 * define the size of the future.
	 */
	public long desiredEvaluationTimespan = 180 * 1000;
	
	/**
	 * Initialize. Read the first line into the buffer
	 * 
	 * @param groundTruthFilename the name of the groundTruth file
	 * @return success
	 */
	public boolean initialize(final String groundTruthFilename) {
		try {
			brImpresssions = new BufferedReader(new FileReader(groundTruthFilename));
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Retrieve a new line from the file and refill the cache.
	 * @return success 
	 * @throws Exception probably a fileHandling problem.
	 */
	public boolean adaptBufferForNewTimeStamp(final long timeStamp) throws Exception{
		
		// delete lines from the buffer, older than the current timeStamp
		if (!listLogFileEntries.isEmpty()) {
			Long oldestTimeStamp = listLogFileEntries.getFirst().getTimeStamp();
			
			while (oldestTimeStamp < timeStamp && !listLogFileEntries.isEmpty()) {
				CacheEntry ce = listLogFileEntries.removeFirst();
				validityByKey.remove(ce.getDuplicateCheckString());
				oldestTimeStamp = listLogFileEntries.getFirst().getTimeStamp();
			}
		}
		
		// fill the buffer with additional lines from the buffer
		Long newestTimeStamp = listLogFileEntries.isEmpty()? timeStamp : listLogFileEntries.getLast().getTimeStamp();
		long requestedNewestTimeStamp = timeStamp  + desiredEvaluationTimespan;
		while (newestTimeStamp < requestedNewestTimeStamp && moreLinesToRead) {
			
			// read the newest line
			String line = brImpresssions.readLine();
			
			// if end of file is reached
			if (line == null) {
				moreLinesToRead = false;
				return false;
			}
			
			// ignore comments
			if (line.startsWith("#")) {
				continue;
			}
			
			// parse the new line and add the line to the cache
			Impression impression = Impression.createImpressionFrom4CollCSV(line);
			CacheEntry ce = new CacheEntry(impression.getUserID(), impression.getItemID(), impression.getDomainID(), impression.getTimeStamp());
			listLogFileEntries.offer(ce);
			newestTimeStamp = listLogFileEntries.getLast().getTimeStamp();
			
			// put the key data extracted from the line in the cache
			String key = ce.getDuplicateCheckString();
			
			// do not reward invalid userIDs and itemIDs
			// store only valid cacheItems
			if (ce.getItemID() > 0 && ce.getUserID() > 0) {
				validityByKey.put(key, Boolean.TRUE);
			}	
		}
		return true;	
	}
	
	/**
	 * Check entry in range. If the entry has been found, mark the entry as found
	 * @param ce CacheEntry
	 * @param blacklisted items 
	 * @return true, if the item is clicked in the next maxTimeDiff period and the item is not blacklisted.
	 */
	public boolean checkPrediction(final CacheEntry ce, final HashSet<Long> blackListedItems) {
		
		boolean endOfEvaluationReached = false;
		try {
			endOfEvaluationReached = adaptBufferForNewTimeStamp(ce.getTimeStamp());
		} catch (Exception e) {
			e.printStackTrace();
		}
		boolean result = containsInTheFuture(ce, blackListedItems);
		if (result) {
			declareInvalid(ce.getDuplicateCheckString());
		}
		return result;
	}
	
	/**
	 * Check entry in range.
	 * @param ce CacheEntry
	 * @param blacklisted items 
	 * @return true, if the item is clicked in the next maxTimeDiff period and the item is not blacklisted.
	 */
	public boolean containsInTheFuture(final CacheEntry ce, final HashSet<Long> blackListedItems) {
		
		if (blackListedItems != null && blackListedItems.contains(ce.getItemID())) {
			return false;
		}
		return Boolean.TRUE.equals(validityByKey.get(ce.getDuplicateCheckString()));
	}
	
	/**
	 * are there still enough data to read
	 * @return
	 */
	public boolean hasMoreLines() {
		return this.moreLinesToRead;
	}

	/**
	 * Remove a key from the map. Ensure that recommending the same over several minutes is rewarded for every line.
	 * @param key the Key of the entry that is invalid
	 */
	public void declareInvalid(final String duplicateCheckString) {
		this.validityByKey.put(duplicateCheckString, Boolean.FALSE);
	}
	
	/**
	 * Close the cache, release the file handle.
	 * @throws IOException
	 */
	public void close() throws IOException {
    	if (brImpresssions != null) {
    		brImpresssions.close();
    	}
	}
	
	@Override
	protected void finalize() throws Throwable {
		try {
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		super.finalize();
	}
	
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	
	public static class CacheEntry { 

		final Long userID;
		final Long itemID;
		final Long domainID;
		final long timeStamp;
		boolean counted = false;
		
		public CacheEntry(final Long userID, final Long itemID, final Long domainID, final long timeStamp) {
			
			this.userID = userID;
			this.itemID = itemID;
			this.domainID = domainID;
			this.timeStamp = timeStamp;
			
		}

		/**
		 * @return the userID
		 */
		public final Long getUserID() {
			return userID;
		}

		/**
		 * @return the itemID
		 */
		public final Long getItemID() {
			return itemID;
		}

		/**
		 * @return the domainID
		 */
		public final Long getDomainID() {
			return domainID;
		}

		/**
		 * @return the timeStamp
		 */
		public final long getTimeStamp() {
			return timeStamp;
		}

		/**
		 * @return the counted
		 */
		public final boolean isCounted() {
			return counted;
		}

		/**
		 * Generate a key for the duplicate check
		 * @return
		 */
		public String getDuplicateCheckString() {
			return userID + "|" + itemID + "|" + domainID;
		}
	}
}
