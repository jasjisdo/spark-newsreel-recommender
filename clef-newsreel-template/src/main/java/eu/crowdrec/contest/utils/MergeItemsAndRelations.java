package eu.crowdrec.contest.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.SequenceInputStream;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.sound.midi.SysexMessage;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * This class analysis the timeStamps in items and relationship files and tries to find the perfect match.
 * 
 * @author andreas
 *
 */
public class MergeItemsAndRelations {

	/**
	 * The main method for testing.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		// the configuration: the program supports files and directories; directories are handled by the sequenceFileReader
		// in addition, gz-files (in directories and separately are supported)
		final String itemFileName = "D:/tmp/CLEF-2015Dataset2/Json-TMP/";//2014-07-01.items.gz";
		final String relationFileName = "D:/tmp/CLEF-2015Dataset2/Json-TMP/";//2014-07-01.data.gz";
		final String itemOutputFileName = "d:/dateFixTestOutput.txt";
		
		// parser for item dates: plista delivers the dates as string, idomaar uses a unix timestamp
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy'-'MM'-'dd' 'kk':'mm':'ss");
		
		// define the global members
		LinkedMap<String> itemMap = new LinkedMap<String>();
		
		HashMap<Long, Long> userMap = new HashMap<Long, Long>(); // userID -> timeStamp (the first interaction with the user) 
		
		// the first big loops reads all items into the main memory
		// compared with the data (user-item-interaction) files, the items are small
		// the item information are kept in a hashTable, having a key created by articleID and domainID
		// open item file
		int lineCounter = 0;
		BufferedReader brItemFile = null;
		try {
			
			// support a list of files in a directory
			File itemFile = new File(itemFileName);
			InputStream is;
			if (itemFile.isFile()) {
				is = new FileInputStream(itemFile);
				// support gZip files
				if (itemFile.getName().toLowerCase().endsWith(".gz")) {
					is = new GZIPInputStream(is);
				}
			}
			else {
				// if the input is a directory, consider all files based on a pattern
				File[] childs = itemFile.listFiles(new FilenameFilter() {
					
					@Override
					public boolean accept(File dir, String name) {
						final String fileName = name.toLowerCase();
						return fileName.endsWith("item.idomaar.txt.gz") || fileName.endsWith("items.gz");
					}
				});
				if (childs == null || childs.length == 0) {
					throw new IOException("invalid itemFileName or empty directory");
				}
				Arrays.sort(childs, new Comparator<File>() {

					@Override
					public int compare(File o1, File o2) {
						return o1.getName().compareTo(o2.getName());
					}
				});
				Vector<InputStream> isChilds = new Vector<InputStream>();
				for (int i = 0; i< childs.length; i++) {
					InputStream tmpIS = new FileInputStream(childs[i]);
					// support gZip files
					if (childs[i].getName().toLowerCase().endsWith(".gz")) {
						tmpIS = new GZIPInputStream(tmpIS);
					}
					isChilds.add(tmpIS);
				}
				is = new SequenceInputStream(isChilds.elements());		
			}
			
			brItemFile = new BufferedReader(new InputStreamReader(is));
			for (String line = brItemFile.readLine(); line != null; line = brItemFile.readLine()) {
	
				String domainID = null;
				String articleID = null;
				Long updatedAt = null;
				
				try {
					String updatedAtString = null;
					// parse the line
					final JSONObject job= (JSONObject) JSONValue.parse(line);
					
					domainID = job.get("domainid") + "";
					articleID = job.get("id") + "";
					updatedAtString  = job.get("updated_at") + "";
					if (updatedAtString == null) {
						updatedAtString = job.get("created_at") + "";
						System.out.println("updated_at is missing set to: " + updatedAtString + " itemID: " + articleID);
					}
					updatedAt = sdf.parse(updatedAtString).getTime();
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (domainID == null || articleID == null || updatedAt == null) {
					System.out.println("invalid line: " + line);
					continue;
				}

				
				String key = domainID + "|" + articleID;
				Object[] value = new Object[]{domainID, articleID, updatedAt, Long.MAX_VALUE, 0L};
				
				// store the line in the buffer
				itemMap.add(key, value);
				
				// debug
				if (++lineCounter % 100 == 0) {
					System.out.println("[INFO] " + lineCounter + " lines processed");
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (brItemFile != null) {
				try {
					brItemFile.close(); 
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
//		if (System.currentTimeMillis() > 0) {
//			System.out.println("Program finished");
//			System.exit(1);
//		}
		
		// create a statistic for the matching
		// matchCounter[0] - relationship line could not be parsed
		// matchCounter[1] - invalid articleID (null or 0)
		// matchCounter[2] - interaction with an unknown item
		// matchCounter[3] - valid match
		int[] matchCounter = new int[4];
		
		// we analyze all relationship-files
		// the data-files are too large for keeping them in main memory, thus we use a streaming approach
		// we check the user-item interactions against the item hashMap
		// in values {domainID, articleID, updatedAt, Long.MAX_VALUE, 0L} we store the minimal timeStamp at value[3]
		// if an interaction with an unknown item has been detected we add a new item in the item-Table, using a 1 at value[4].
		
		// open relationship file
		BufferedReader brRelationFile = null;
		try {
			
			File relationFile = new File(relationFileName);
			
			InputStream isRelationFile;
			if (relationFile.isFile()) {
				isRelationFile = new FileInputStream(relationFile);
				// support gZip files
				if (relationFile.getName().toLowerCase().endsWith(".gz")) {
					isRelationFile = new GZIPInputStream(isRelationFile);
				}
			}
			else {
				// if the input is a directory, consider all files based on a pattern
				File[] childs = relationFile.listFiles(new FilenameFilter() {
					
					@Override
					public boolean accept(File dir, String name) {
						final String fileName = name.toLowerCase();
						return fileName.endsWith("data.idomaar.txt.gz") || fileName.endsWith("data.gz");
					}
				});
				if (childs == null || childs.length == 0) {
					throw new IOException("invalid dataFileName or empty directory");
				}
				Arrays.sort(childs, new Comparator<File>() {

					@Override
					public int compare(File o1, File o2) {
						return o1.getName().compareTo(o2.getName());
					}
				});
				Vector<InputStream> isChilds = new Vector<InputStream>();
				for (int i = 0; i< childs.length; i++) {
					InputStream tmpIS = new FileInputStream(childs[i]);
					// support gZip files
					if (childs[i].getName().toLowerCase().endsWith(".gz")) {
						tmpIS = new GZIPInputStream(tmpIS);
					}
					isChilds.add(tmpIS);
				}
				isRelationFile = new SequenceInputStream(isChilds.elements());		
			}
			
			brRelationFile = new BufferedReader(new InputStreamReader(isRelationFile));
			for (String line = brRelationFile.readLine(); line != null; line = brRelationFile.readLine()) {
				
				// parse the line
				String domainID = null;
				String articleID = null;
				Long userID = null;
				String timeStampString = null;
				
				try {
					// parse the line
					final JSONObject job= (JSONObject) JSONValue.parse(line);
					final JSONObject jobC= (JSONObject) job.get("context");
					final JSONObject jobS= (JSONObject) jobC.get("simple");
					
					domainID = jobS.get("27") + "";
					articleID = jobS.get("25") + "";
					timeStampString  = job.get("timestamp") + "";
					userID = Long.valueOf(jobS.get("57")+"");
				
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (domainID.equals("null") || timeStampString.equals("null")) {
					System.out.println("invalid line in the relationFile: " + line);
					matchCounter[0]++;
					continue;
				}
				if (articleID.equals("null") || articleID.equals("0")) {
					matchCounter[1]++;
					continue;
				}
				
				String key = domainID + "|" + articleID;
				
				// store the line in the buffer
				Object[] value = itemMap.get(key);
				
				// check the itemEntry
				if (value == null) {
					System.out.println("interaction with an unknown item: " + domainID + "|" + articleID);
					matchCounter[2]++;
					// we create a new item Entry
					Object[] tmpValue = new Object[]{domainID, articleID, Long.valueOf(timeStampString), Long.valueOf(timeStampString), 1L};
					itemMap.add(key, tmpValue);
				} else {
					// check whether we should update the information in the item table
					value[3] = Math.min((Long) value[3],  Long.valueOf(timeStampString));
					matchCounter[3]++;
				}
				
				
				/// update the userStatistic
				Long userTimeStamp = userMap.get(userID);
				if (userTimeStamp == null) {
					userMap.put(userID, Long.valueOf(timeStampString));
				} else {
					long tmpTimeStamp = Long.valueOf(timeStampString).longValue();
					if (tmpTimeStamp < userTimeStamp) {
						userMap.put(userID, Long.valueOf(tmpTimeStamp));
						// System.out.println("Unexpected timestamp in the userMap: userID=" + userID + " newTimeStamp=" + timeStampString + " oldTimeStamp=" + userTimeStamp);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (brItemFile != null) {
				try {
					brItemFile.close(); 
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		// analyze the relations without items
		System.out.println(Arrays.toString(matchCounter));
		
		// analyze the items without relations
		int[] itemMatchCounter = new int[2];
		for (Map.Entry<String,Object[]> entry : itemMap.getList()) {
			long timeStamp = (Long) entry.getValue()[3];
			if (timeStamp == Long.MAX_VALUE) {
				itemMatchCounter[0]++;
				//System.out.println("unused item: " + Arrays.toString(entry.getValue()));
			} else {
				itemMatchCounter[1]++;
			}
		}
		System.out.println("itemMatchCounter: " + Arrays.toString(itemMatchCounter));
		System.out.println("matchCounter: " + Arrays.toString(matchCounter));
		
		// rewrite the item files 
		// rewriteTheItemFiles(itemFileName, itemMap);
		
		// write the user files
		writeUserFiles(userMap);
		
		// write the missing item files
		writeMissingItemIDsIntoFiles(itemMap);
		
		// debug
		if (System.currentTimeMillis() > 0) {
			return;
		}
		
		// write the results into a new file
		BufferedWriter bwItemInfos = null;
		try {
			bwItemInfos = new BufferedWriter(new FileWriter(itemOutputFileName));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bwItemInfos != null) {
				try {
					bwItemInfos.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		// hmap only contains the oldest timeStamp for each item from itemTable
		// itemID = itemID+|+domainID
		// compute a histogram for the difference of the timestamps from the different sources
	}
	
	
	public static void rewriteTheItemFiles(final String itemFileName, final LinkedMap<String> itemMap) {
		
		try {
			File itemFile = new File(itemFileName);
			File[] childs = itemFile.listFiles(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					final String fileName = name.toLowerCase();
					return fileName.endsWith("items.gz");
				}
			});
			
			// for all childs
			for (File oldItemFile : childs) {
				
				// define a new outputFile
				String newItemFileName = oldItemFile.getName();
				
				BufferedReader brItemFile = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(oldItemFile))));
				BufferedWriter bwItemFile = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(newItemFileName))));
	
				for (String line = brItemFile.readLine(); line != null; line = brItemFile.readLine()) {
					
					// parse the input and search for domainID and itemID
					String domainID = null;
					String articleID = null;
					try {
						final JSONObject job= (JSONObject) JSONValue.parse(line);
						domainID = job.get("domainid") + "";
						articleID = job.get("id") + "";
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (domainID == null || articleID == null) {
						System.out.println("invalid line: " + line);
						continue;
					}

					
					String key = domainID + "|" + articleID;
					Object[] value = itemMap.get(key);
					long relationTimeStamp = (Long) value[3];
					long itemTimeStamp = (Long) value[2];
					long timeStamp;
					if (relationTimeStamp == Long.MAX_VALUE) {
						timeStamp = itemTimeStamp;
					} else {
						timeStamp = relationTimeStamp - 1L;
					}
					
					int pos = line.lastIndexOf("}");
					String newLine = line.substring(0, pos) + ", \"timestamp\":" + (timeStamp-1) + "}";
					System.out.println(newLine);
					
					bwItemFile.write(newLine);
					bwItemFile.newLine();
				}
				bwItemFile.close();
				brItemFile.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * write the user-IDs for the dataset is day-based files
	 * @param userMap
	 */
	public static void writeUserFiles(final Map<Long, Long> userMap) {
		
		
		// sort the entries in a list
		List<Map.Entry<Long, Long>> list = new ArrayList<Map.Entry<Long, Long>>(userMap.size());
		list.addAll(userMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Long, Long>>() {

			@Override
			public int compare(Entry<Long, Long> o1, Entry<Long, Long> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}
		});
		
		// find the required fileNames based on the timeStamps
		final SimpleDateFormat sdf0 = new SimpleDateFormat("yyy'-'MM'-'dd");
		String outFilePrefix = "user.idomaar.txt";
		Map<String, BufferedWriter> bufferedWriterByID = new HashMap<String, BufferedWriter>();
		try {
			// for all userIDs
			for (Entry<Long, Long> entry : list) {
				
				// compute the number and the fileName for the userTimestamp
				final Date tmpDate = new Date(entry.getValue());
				String monthString = sdf0.format(tmpDate);
				
				// get the relevant bufferedWriter
				BufferedWriter bw = bufferedWriterByID.get(monthString);
				if (bw == null) {
					bw = new BufferedWriter(new FileWriter(monthString + "-" + outFilePrefix));
					System.out.println("writing: " + monthString + "-" + outFilePrefix);
					bufferedWriterByID.put(monthString, bw);
				}
				
				bw.write("user\t" + entry.getKey() + "\t" + entry.getValue() + "\t{}\t{}\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			for (Entry<String, BufferedWriter> entry : bufferedWriterByID.entrySet()) {
				try {
					entry.getValue().close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * write the user-IDs for the dataSet is day-based files
	 * @param userMap
	 */
	public static void writeMissingItemIDsIntoFiles(final LinkedMap<String> itemMap) {
		
		
		// define a new list and copy the relevant values to a new list
		List<Object[]> list = new ArrayList<Object[]>(itemMap.getList().size() / 50);
		for (Object[] entryValue : itemMap.getMap().values()) {
			if ( (Long) entryValue[4] == 1L) {
				list.add(entryValue);
			}
		}

		// sort based on the timeStamps
		Collections.sort(list, new Comparator<Object[]>() {

			@Override
			public int compare(Object[] o1, Object[] o2) {
				return ((Long) o1[3]).compareTo((Long) o2[3]);
			}
		});

		// find the required fileNames based on the timeStamps
		final SimpleDateFormat sdf0 = new SimpleDateFormat("yyy'-'MM'-'dd");
		String outFilePrefix = "items2.idomaar.txt";
		Map<String, BufferedWriter> bufferedWriterByID = new HashMap<String, BufferedWriter>();
		try {
			System.out.println("plan to process entities: " + list.size());
			// for all userIDs
			for (Object[] entry : list) {
				
				// compute the number and the fileName for the userTimestamp
				final Date tmpDate = new Date((Long) entry[3]);
				String monthString = sdf0.format(tmpDate);
				
				// get the relevant bufferedWriter
				BufferedWriter bw = bufferedWriterByID.get(monthString);
				if (bw == null) {
					bw = new BufferedWriter(new FileWriter(monthString + "-" + outFilePrefix));
					System.out.println("writing: " + monthString + "-" + outFilePrefix);
					bufferedWriterByID.put(monthString, bw);
				}
				
				bw.write(
						"item-created\t" + 
						entry[1] + "\t" + 
						entry[3] + "\t" +
						"{\"domainid\": "+ entry[0] + ", \"img\": \"\", \"title\": \"\", \"url\": \"\", \"text\": \"\", \"created_at\": \"" + monthString + " 00:00:00\",  \"version\": 1, \"timestamp\":" + (Long) entry[3] + "}\t" + 
						"{\"itemID\":" + entry[1] +", \"domainID\":" + entry[0] + "}" +
						"\n"
				);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			for (Entry<String, BufferedWriter> entry : bufferedWriterByID.entrySet()) {
				try {
					entry.getValue().close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	/**
	 * A linked hashMap providing extended sorting of entries
	 * The value type is an object array, add operations effect the field value[3].
	 * 
	 * @author andreas
	 *
	 * @param <K>
	 */
	public static class LinkedMap<K> {
		
		private final Map<K, Object[]> hmap = new HashMap<K, Object[]>();
		private final List<Map.Entry<K,Object[]>> llist = new LinkedList<Map.Entry<K,Object[]>>();
		
		/**
		 * Default Constructor
		 */
		public LinkedMap() {
			super();
		}
		
		public void add(K key, Object[] value) {
			Object[] oldValue = hmap.get(key);
			if (oldValue == null) {
				hmap.put(key, value);
				llist.add(new AbstractMap.SimpleEntry<K, Object[]>(key, value));
			} else {
				if ((Long) oldValue[3] > (Long) value[3]) {
					System.out.println("unexpected order in item file");
				}
				oldValue[3] = Math.min((Long) oldValue[3], (Long) value[3]);
			}
		}
		
		public Object[] get(K key) {
			return hmap.get(key);
		}
		
		public List<Map.Entry<K,Object[]>> getList() {
			return this.llist;
		}
		
		public Map<K, Object[]> getMap() {
			return this.hmap;
		}
	}
}
