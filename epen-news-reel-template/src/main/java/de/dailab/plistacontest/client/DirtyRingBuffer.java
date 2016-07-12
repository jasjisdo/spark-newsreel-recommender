/*
Copyright (c) 2013, TU Berlin
Permission is hereby granted, free of charge, to any person obtaining 
a copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
DEALINGS IN THE SOFTWARE.
*/

package de.dailab.plistacontest.client;

import java.util.*;

/**
 * A HashMap providing a ring buffer for each key. This implementation shall illustrate a simple baseline recommender. 
 * A fixed number of items is inserted into a list. The items are recommended in reversed chronological order. When 
 * the list reaches its pre-defined size, the least recent items are dropped. Thus, the recommender considers recency 
 * as most important factor.
 * 
 * @author andreas
 *
 * @param <K> the type of the key
 * @param <V> the type of the values in the ring buffer
 */
public class DirtyRingBuffer<K, V> {

	/**
	 * store a list of values for each key.
	 */
	private final Map<K, Object[]> listByKey = new HashMap<K, Object[]>();

	/**
	 * store the write index positions for each key.
	 */
	private final Map<K, int[]> listIndexByKey = new HashMap<K, int[]>();

	/**
	 * the maximal number of items per key.
	 */
	private int maximalNumberOfItemsPerKey;

	/**
	 * Constructor.
	 * 
	 * @param maximalNumberOfItemsPerKey
	 **/
	public DirtyRingBuffer(final int maximalNumberOfItemsPerKey) {
		super();
		this.maximalNumberOfItemsPerKey = maximalNumberOfItemsPerKey;
	}

	/**
	 * Add a new value for a key
	 * If there is no ring buffer for the key, create a ring buffer
	 * The add method is synchronized in order to ensure that all values are inserted correctly
	 * key refers to the publisher id (context.simple.27)
	 * 
	 * @param key
	 *            the key
	 * @param value
	 *            the value
	 */
	@SuppressWarnings("unchecked")
	public void addValueByKey(final K key, final V value) {

		if (key == null) {
			throw new RuntimeException("invalid arguments k=" + key);
		}

		//synchronized (this) 
		{
			V[] currentList = (V[]) this.listByKey.get(key);
			if (currentList == null) {
				currentList = (V[]) new Object[maximalNumberOfItemsPerKey];
				this.listByKey.put(key, currentList);
				int[] listIndex = new int[] { 0 };
				this.listIndexByKey.put(key, listIndex);
			}

			int[] listIndex = this.listIndexByKey.get(key);
			currentList[listIndex[0]] = value;
			listIndex[0] = (++listIndex[0]) % maximalNumberOfItemsPerKey;
		}
	}
	
	/**
	 * Get some values from the ring buffer.
	 * This method is NOT synchronized and may return inconsistent values.
	 * This method does not block. It always returns an HashSet (might be empty)
	 * values refer to all items which are in the publisher's item list
	 * 
	 * @param key
	 * @param numberOfValues
	 * @param _blackListedIDs
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Set<V> getValuesByKey(final K key, final int numberOfValues, final Set<V> _blackListedIDs) {

		if (key == null) {
			throw new RuntimeException("invalid arguments k=" + key);
		}

		// define the result
		Set<V> result = new HashSet<V>();
		
		// determine the current write position
		int[] temp = this.listIndexByKey.get(key);
		
		// search backward in the ring buffer starting just at the last written position
		if (temp != null){
			int currentIndex =  temp[0];
			
			// limit the number of search steps
			// since the lists may contain duplicates, searches may needs more steps
			for (int i = 0; i < 3 * numberOfValues; i++) {
				
				// determine the last position
				if (currentIndex <= 0) {
					currentIndex = maximalNumberOfItemsPerKey;
				}
				currentIndex = currentIndex-1;
				V currentObject = (V) this.listByKey.get(key)[currentIndex];
				// the itemID may not be null, already contained in the result list, or contained in the black list
				if (currentObject != null && !result.contains(currentObject) && !_blackListedIDs.contains(currentObject)) {
					result.add(currentObject);
				}
				
				if (result.size() >= numberOfValues) {
					break;
				}
			}
		}
		return result;
	}


	/**
	 * Get a nice string representation for debugging
	 * 
	 * @see Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("DRB:");
		for (Map.Entry<K, Object[]> entry : this.listByKey.entrySet()) {
			sb.append(entry.getKey() + ":" + Arrays.toString(entry.getValue())
					+ ";" + this.listIndexByKey.get(entry.getKey())[0] + "|");
		}
		return sb.toString();
	}

	/**
	 * A test case checking the functionality.
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {

		System.out.println("Hallo");
		DirtyRingBuffer<String,Integer> myMap = new DirtyRingBuffer<String,Integer>(3);
		myMap.addValueByKey("User0", 1);
		System.out.println(myMap);
		myMap.addValueByKey("User0", 2);
		System.out.println(myMap);
		myMap.addValueByKey("User0", 3);
		System.out.println(myMap);
		myMap.addValueByKey("User0", 4);
		System.out.println(myMap);
		myMap.addValueByKey("User0", 3);
		System.out.println(myMap);
		myMap.addValueByKey("User0", 1);
		System.out.println(myMap);
		myMap.addValueByKey("User1", 10);
		System.out.println(myMap);
		myMap.addValueByKey("User0", 9);
		myMap.addValueByKey("User2", 10);
		System.out.println(myMap);
		myMap.addValueByKey("User2", 1);
		System.out.println(myMap);
		myMap.addValueByKey("User2", 3);
		System.out.println(myMap);
		myMap.addValueByKey("User3", 100);
		System.out.println(myMap);

	}
}
