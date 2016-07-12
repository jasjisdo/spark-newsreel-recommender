package eu.crowdrec.contest.evaluation;

import java.io.*;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import eu.crowdrec.contest.evaluation.LinkedFileCacheDuplicateSupport.CacheEntry;


public class Evaluator {
	
	////////////////////////////////////////////////////////////////////////////
	///// adapt the following line for enabling a detailed evaluation /////////
	////////////////////////////////////////////////////////////////////////////

    /* Default column delimiter.*/
    public final static String DELIM = "\t";

    /*
     * Header domain prefix symbols for detailed output file
     *
     * <pre>
     * The ranking is:
     *      A:      zero-defect recommendations.
     *      B:      (not critical) few-defect recommendations.
     *      C:      defective recommendations.
     *      D:      ratio of successful (clicked) recommendations.
     *      S:      sum of all recommendations.
     * </pre>
     *
     */
    private final static String[] domainHeaderPrefix = new String[]{"A","B","C","D", "S"};

	/**
	 * the fileName for creating a detailed evaluation analysis, set to null for disabling this feature
	 */
	public static String fileLocationOfDetailedEvaluation = null;

	/**
	 * should the response time histogram be printed? - simply set to true if interested in the distribution
	 */
	public final static boolean printResponseTimeHistogram = false;
	
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	
	/**
	 * prevent invalid answers, recommending just everything
	 */
	private static final int MAX_NUMBER_OF_RECOMMENDATIONS = 6;
	
	/**
	 * the set of forbidden items
	 */
	private static final HashSet<Long> blackListedItems = new HashSet<Long>();
	static {
		blackListedItems.add(0L);
	}
	
	/**
	 * aggregate the evaluation results for different domains
	 */
	private final static Map<Long, int[]> resultCount = new HashMap<Long, int[]>();
	
	/**
	 * aggregate the evaluation results for different domains  based on a context specific key
	 */
	private final static Map<String, Map<Long, int[]>> resultCountByContextKey = new TreeMap<String, Map<Long, int[]>>();
	
	/**
	 * the timeStamp to contextKey converter (only relevant, if the detailed evaluation is enabled
	 */
	//private final static SimpleDateFormat sdf01 = new SimpleDateFormat("yy'\t'MM'\t'dd'\t'ww'\t'HH'\t'mm'\t'EE");
	//private final static SimpleDateFormat sdf01 = new SimpleDateFormat("yy'\t'MM'\t'dd'\t'ww'\t'HH'\tmm\t'EE");
	private final static SimpleDateFormat sdf01 = new SimpleDateFormat("yy'\t'MM'\t'dd'\t'ww'\t'HH'\tmm\t'EE'\t'yyy'-'MM'-'dd'-'HH");


	/**
	 * The responseTime statistic.
	 */
	private final static SummaryStatistics responseTimeStatistic = new SummaryStatistics();

	/**
	 * create a histogram for debugging (a detailed response time analysis)
	 */
	private final static int[] histogram = new int [500];
	
	/**
	 * compute a context key based on a timeStamp
	 */
	private static String computeContextKey(long _timeStamp) {
		String tmp = sdf01.format(_timeStamp);
		long q = _timeStamp / 60000L;
		q %= 60L;
		q /= 15L;
		q = 0;
		return tmp + "-" + q;
	}

    /* IDs of domain identifier as Long. */
    private static Set<Long> domainIDs;

	/**
	 * Write a detailed analysis based on the collected context keys to a file.
     *
	 * @param fileLocation location of the output file with results of detailed analysis.
	 */
	private static void writeDetailedStatistic(final String fileLocation) {

        final long[] keys = {596L, 694L, 1677L};
        domainIDs = new HashSet<>(Arrays.asList(ArrayUtils.toObject(keys)));

        // add all in result available domain ids to domainIDs list.
        for (Map.Entry<String, Map<Long, int[]>> entry : resultCountByContextKey.entrySet()) {
            domainIDs.addAll(entry.getValue().keySet());
        }

        BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(fileLocation));
			writeHeaderLine(bw, domainIDs);

			for (Map.Entry<String, Map<Long, int[]>> entry : resultCountByContextKey.entrySet()) {
                writeDetailedResultLine(bw, domainIDs, entry);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

    /**
     * Write a header line on defined writer location.
     *
     * @param w             writer which used to writer static header line.
     * @param domainIDs     set domain ids which will be complected in the header line.
     * @throws IOException  if something went wrong like write error etc.
     */
    private static void writeHeaderLine(Writer w, Set<Long> domainIDs) throws IOException {
        w.write("year\tmonth\tday\tweek\thour\tminute\tweekday\tdate");
        for (long domainID : domainIDs) {
//            w.write("\tB" + domainID );
//            w.write("\tA" + domainID );
//            w.write("\tC" + domainID );
//            w.write("\tD" + domainID );
//            w.write("\tS" + domainID );
            for (int i = 0; i < domainHeaderPrefix.length; i++) {
                w.write(DELIM + domainHeaderPrefix[i] + domainID );
            }
        }
        w.write(System.lineSeparator());
    }

    /**
     * Write a detailed result line with ranked counted recommendations.
     * <pre>
     * The ranking is:
     *      A:      zero-defect recommendations.
     *      B:      (not critical) few-defect recommendations.
     *      C:      defective recommendations.
     *      D:      ratio of successful (clicked) recommendations.
     *      S:      sum of all recommendations.
     * </pre>
     *
     *
     * @param w
     * @param domainIDs
     * @param entry
     * @throws IOException
     */
    private static void writeDetailedResultLine(Writer w, Set<Long> domainIDs,
                                                Map.Entry<String, Map<Long, int[]>> entry) throws IOException {

//        bw.write(entry.getKey() + "\t");
//        for (long domainID : keys) {
//            int[] values = entry.getValue().get(domainID);
//            if (values == null) {
//                bw.write("0\t0\t0\t0\t");
//            } else {
//                bw.write(values[0] + "\t" + values[1] + "\t" + values[2] + "\t" + ((double)values[0] / (values[0] + values[1] + 6 * values[2])) + "\t" + (values[0] + values[1] + 6 * values[2]) + "\t");
//            }
//        }
//        bw.newLine();

        w.write(entry.getKey() + "\t");
        for (long domainID : domainIDs) {
            int[] values = entry.getValue().get(domainID);
            if (values == null) {
                //w.write("-0\t-0\t-0\t-0.0\t-0.0\t");
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < domainHeaderPrefix.length; i++) {
                    builder.append("-0");
                    builder.append(DELIM);
                }
                w.write(builder.toString());
            } else {
                StringBuilder builder = new StringBuilder();

                builder.append(values[0]);  // write (A) zero-defect recommendations.
                builder.append(DELIM);
                builder.append(values[1]);  // write (B) (not critical) few-defect recommendations.
                builder.append(DELIM);
                builder.append(values[2]);  // write (C) defective recommendations.
                builder.append(DELIM);
                builder.append(((double)values[0] / (values[0] + values[1] + 6 * values[2])));
                builder.append(DELIM);
                builder.append((values[0] + values[1] + 6 * values[2]) + "\t");

                w.write(builder.toString());
            }
        }
        w.write(System.lineSeparator());

    }

	/**
	 * Return the log entry for the requested context key an the domain.
	 * @param _contextKey typically a timeStamp
	 * @param _domainID the domainID
	 * @return int-array a log entry 
	 */
	private static int[] getResultCountEntry(final long _contextKey, final Long _domainID, final String _prop) {
		
		// String contextKey = _prop; // computeContextKey(_contextKey);
		String contextKey = computeContextKey(_contextKey);
		Map<Long, int[]> countEntryForContext = resultCountByContextKey.get(contextKey);
		if (countEntryForContext == null) {
			resultCountByContextKey.put(contextKey, new HashMap<Long, int[]>());
			countEntryForContext = resultCountByContextKey.get(contextKey);
		}
		int[] countEntryForDomainAndContext = countEntryForContext.get(_domainID);
		if (countEntryForDomainAndContext == null) {
			countEntryForContext.put(_domainID, new int[3]);
			countEntryForDomainAndContext = countEntryForContext.get(_domainID);
		}
		return countEntryForDomainAndContext;
	}
  
	/**
	 * Run the evaluation process. Ensure that enough heap is available for caching.
	 * The amount of required memory is linear in the number of cached lines / the size of the time window
	 *   considered in the evaluation.
	 *     
	 * @param args the files used in the evaluation.
	 * @throws IOException indicates a problem while searching or opening the ground truth file(s)
	 */
	public static void main(String[] args) throws IOException {
		
		// define default settings for simplified testing
		String predictionFileName = "";
		String groundTruthFileName = "";

		// define the default window size
		long windowSizeInMillis = 5L * 60L * 1000L;
		boolean activateOrpInternalLogFormat = false;
		
		// check the parameters
		if (args.length < 0 || args.length > 4) {
			System.out.println("usage: java Evaluator <predictionFileName> <groundTruthFileName> [<windowSizeInMillis>]");
			//System.exit(0);
			System.out.println(".. using the default values.");
		}
		
		// set the parameter values
		if (args.length > 0) {
			predictionFileName = args[0];
		}
		if (args.length > 1) {
			groundTruthFileName = args[1];
		}
		if (args.length > 2) {
			windowSizeInMillis = Long.parseLong(args[2]);
		}
		if (args.length > 3 && args[3].charAt(0) == 't') {
			activateOrpInternalLogFormat = true;
			System.out.println("activateOrpInternalLogFormat is activated!");
		}

        // activate detailed logging with extra param.
        if(args.length > 4) {
            fileLocationOfDetailedEvaluation = args[4];
            File file = new File(fileLocationOfDetailedEvaluation);
            if(file.exists()) {
                if (!(file.canWrite())) {
                    System.err.println("No write permission for detailed output file");
                    System.exit(3);
                }
            } else {
                if (!(file.getParentFile().canWrite())) {
                    System.err.println("No write permission to create detailed output file");
                    System.exit(4);
                }
            }
        }
		
		// inform the user that the evaluator has started
		System.out.println("Evaluation is running ...");
		System.out.println("predictionFileName= " + predictionFileName);
		System.out.println("groundTruthFileName= " + groundTruthFileName);
		System.out.println("windowSizeInMillis= " + windowSizeInMillis);
		
		// initialize the groundTruth linked list
		LinkedFileCacheDuplicateSupport lfc = new LinkedFileCacheDuplicateSupport();
		lfc.initialize(groundTruthFileName, windowSizeInMillis, activateOrpInternalLogFormat);

		// initialize the prediction list that should be evaluated
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(predictionFileName));
			for (String line = br.readLine(); line != null; line = br.readLine()) {

				try {
					// ignore comments and invalid lines
					if (line.length() < 2 || line.startsWith("#")) {
						continue;
					}
					
					// try to parse the prediction line
					String[] token = line.split("\t");
					
					long messageID = Long.parseLong(token[1]);
					long timeStamp = Long.parseLong(token[2]);
					long responseTime = Long.parseLong(token[3]);
					responseTimeStatistic.addValue(responseTime);
					int tmpResponseTime = (int) (responseTime/10);
					if (tmpResponseTime >= histogram.length) {
						tmpResponseTime = histogram.length-1;
					}
					histogram[tmpResponseTime]++;
					//long itemID = Long.parseLong(token[4]);
					
					long userID = -1;
					try {
						userID = Long.parseLong(token[5]);
					} catch (Exception ignored) {
					}
					
					long domainID = -2;
					try {
						domainID = Long.parseLong(token[6]);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					boolean recommendationAvailable = token.length > 7;
					if (recommendationAvailable) {
						try {
							String recommendations = token[7];
							final JSONObject jsonObj = (JSONObject) JSONValue.parse(recommendations);
							
							JSONObject recs = (JSONObject) jsonObj.get("recs");
							JSONObject recsInts = (JSONObject) recs.get("ints");
							JSONArray itemIds = (JSONArray) recsInts.get("3");
							if (itemIds != null) {
								for (int i = 0;  i < MAX_NUMBER_OF_RECOMMENDATIONS; i++) {
									Long itemID = 
										i < itemIds.size() 
										? Long.parseLong(itemIds.get(i) + "")
										: 0L;
									
									// check the IDs
									CacheEntry ce = new CacheEntry(userID, itemID, domainID, timeStamp);
									boolean valid = lfc.checkPrediction(ce, blackListedItems);
									
									//System.out.println("checking:\t" + timeStamp + "\t" + userID + "\t" + domainID + "\t" + itemID + "\t:" + valid) ;
		
									int[] countEntry = resultCount.get(domainID);
									if (countEntry == null) {
										resultCount.put(domainID, new int[3]);
										countEntry = resultCount.get(domainID);
									}

									int[] countEntryForDomainAndContext = getResultCountEntry(timeStamp, domainID, token[8]);
									if (valid) {
										countEntry[0]++;
										countEntryForDomainAndContext[0]++;
									} else {
										countEntry[1]++;
										countEntryForDomainAndContext[1]++;
									}
								}
							}
						} catch (Exception e) {
							// we assume that no valid recommendation has been provided
							recommendationAvailable = false;
						}
					} // end recommendation available
					if (!recommendationAvailable) {
						//System.out.println("recommendation missing for domain " + domainID);
						int[] countEntry = resultCount.get(domainID);
						if (countEntry == null) {
							resultCount.put(domainID, new int[3]);
							countEntry = resultCount.get(domainID);
						}
						// we count the number of invalid responses
						countEntry[2]++;
						
						int[] countEntryForDomainAndContext = getResultCountEntry(timeStamp, domainID, null /*token[8]*/);
						countEntryForDomainAndContext[2]++;
					}

				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("invalid line: " + line);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (Exception ignored) {
				}
			}
		}

		// close and cleanup
		try {
			lfc.close();
		} catch (IOException ignored) {
		}
		
		// printout the results
		int[] overall = new int[3];
		System.out.println("\nEvaluation results\n==================");
		for (Map.Entry<Long, int[]> entry: resultCount.entrySet()) {
			int[] values = entry.getValue();
			System.out.println(entry.getKey() + DELIM + Arrays.toString(values) + DELIM + NumberFormat.getInstance().format(1000*values[0] / (values[0]+values[1]+MAX_NUMBER_OF_RECOMMENDATIONS*values[2])) + " o/oo");
			for (int i = 0; i < values.length; i++) {
				overall[i] += values[i];
			}
		}
		System.out.println("all" + DELIM + Arrays.toString(overall) + DELIM + NumberFormat.getInstance().format(1000*overall[0] / (overall[0]+overall[1]+MAX_NUMBER_OF_RECOMMENDATIONS*overall[2])) + " o/oo");
		System.out.println(
				"mean/min/max/stdDev/n" + DELIM + 
				responseTimeStatistic.getMean() + DELIM + 
				responseTimeStatistic.getMin() + DELIM + 
				responseTimeStatistic.getMax() + DELIM + 
				responseTimeStatistic.getStandardDeviation() + DELIM + 
				responseTimeStatistic.getN());
		
		// write a context-key specific evaluation file
		if (fileLocationOfDetailedEvaluation != null) {
			writeDetailedStatistic(fileLocationOfDetailedEvaluation);
		}
		
		// print the histogram for the response time statistic
		if (printResponseTimeHistogram) {
			System.out.println("'==Histogram==");
			for (int i = 0; i < histogram.length; i++) {
				System.out.println((i*10) + DELIM + histogram[i]);
			}
		}
	}
}
