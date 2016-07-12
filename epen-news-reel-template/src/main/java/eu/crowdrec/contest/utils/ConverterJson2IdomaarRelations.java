package eu.crowdrec.contest.utils;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Convert the CLEF-data files into the idomaar data format.
 * 
 * @author andreas
 *
 */
public class ConverterJson2IdomaarRelations {

	/**
	 * The parser for the date format.
	 */
	final static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	/**
	 * The method for converting the ORP-log file into the Idomaar file format
	 * 
	 * @param sourceFileName the name of the source file (the file should be formatted in JSON)
	 * @param targetFile the name of the target file (the file will be formatted in the Idomaar 5-column format.
	 */
	public static void converter(final String sourceFileName, final String targetFileName) {
		
		// the separator for the csv file, typically a tab char
		final String separator = "\t";
		
		// define the file descriptors
		BufferedReader br = null;
		BufferedWriter bw = null;
		
		try {
			// initialize the reader and writer - since the source file should be in ASCII, we do not care about the encoding
			br = new BufferedReader(new FileReader(sourceFileName));
			bw = new BufferedWriter(new FileWriter(targetFileName));
			
			String line = "";
			int count = 0;
			try {
				for (line = br.readLine(); line != null; line = br.readLine()) {
					
					// catch unacceptable lines.
					if (line.startsWith("null") || line.length() < 5) {
						System.err.println("[WARNING] invalid line detected, ignored. line (" + count + ")=" + line);
						continue;
					}
					
					// in order to be compatible with old log files, we try to split the line at <TABS>
					String[] token = line.split("\t");
					
					String messageType;
					String messageBody;

					if (token.length == 3)  {
						// old format
						messageType = token[0];
						messageBody = token[1];
						
						// time stamp and number
						Long timestamp = 0L;
						int number = 0;
						String timeStr = token[1].substring(token[1]
								.lastIndexOf("}") + 2, token[1].length());
						number = Integer.parseInt(timeStr.substring(timeStr
								.lastIndexOf(",") + 1));
						timeStr = timeStr.substring(0, timeStr.lastIndexOf(","));
						Date d = null;
						try {
							d = df.parse(timeStr);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						if (d != null) {
							timestamp = d.getTime();
						}
						System.out.println(timestamp + "\t" + number);
				
					} else {
						// plain JSON format
						messageType = null;
						messageBody = token[0];
					}

					// parse the message body using a JSON parser
					final JSONObject jsonObj = (JSONObject) JSONValue.parse(messageBody);

					// extract the timeStamp
					Object timeStampJSON = jsonObj.get("timestamp");
					//System.out.println("timeStamp = " + timeStampJSON);

					// extract the messageType
					messageType = "impression";
					Object messageTypeJSON = jsonObj.get("recommendation_request");
					if (messageTypeJSON != null && "true".equalsIgnoreCase(messageTypeJSON+"")) {
						messageType = "request";
					}
					//System.out.println("messageType = " + messageType);
					
					// extract the userID
					JSONObject jsonObjContext = (JSONObject) jsonObj.get("context");
					JSONObject jsonObjSimple = (JSONObject) jsonObjContext.get("simple");
					String userID = jsonObjSimple.get("57") + "";
					//System.out.println("userID = " + userID);
					
					// extract the itemID
					Object itemIdTmp = jsonObjSimple.get("25");
					String itemID = itemIdTmp == null? "0" : itemIdTmp + "";
					//System.out.println("itemID = " + itemID);					

					// extract the itemID
					String domainID = jsonObjSimple.get("27") + "";
					//System.out.println("domainID = " + domainID);			
					
					// create the relationshipID
					long rid = System.currentTimeMillis() % (10000000L) * 1000000;
					long offset0 = (count % 100) * 1000;
					long offset1 = Long.parseLong(domainID);
					rid += offset0 + offset1;
					
					// write output
					bw.write(messageType + separator);
					bw.write(rid + separator);
					bw.write(timeStampJSON + separator);
					bw.write(messageBody+ separator);
					bw.write("{\"userID\":" + userID +", \"itemID\":" + itemID + ", \"domainID\":" + domainID + "}" + separator);
					bw.newLine();
					
					// report the progress
					count++;
					if (count % 100000 == 0) {
						bw.flush();
						System.out.println(count + " have been converted.");
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			System.out.println("log file does not exist.");
		} catch (IOException e) {
			System.out.println("can't create a new file: " + targetFileName);
		} finally {
			try {
				br.close();
				bw.close();
			} catch (IOException e) {
				System.out.println(e.toString());
			}
		}
	}


	/**
	 * The main method analyzes the arguments and call the convert method.
	 * @param args args[0] - sourceFileName, args[1] - targetFileName (optional) 
	 */
	public static void main(String[] args) {
		
		// check the number of arguments
		if (args.length != 1 && args.length != 2) {
			System.out.println("invalid number of arguments.");
			System.out.println("usage: java ConverterJson2IdomaarRelations sourceFileName [targetFileName].");
			System.exit(0);
		}
		
		// extract the file names
		String sourceFileName = args[0];
		String targetFileName;
		if (args.length > 1) {
			targetFileName = args[1];
		} else {
			targetFileName = args[0] + ".idomaar.txt";
		}
		converter(sourceFileName, targetFileName);
	}

	/*
	 * an unused variable might used for debugging in the future - visualizes the data format.
	 */
	private static final String exampleMessageBody = 
		"{\"recs\": {\"ints\": {\"3\": [147471692, 147034641, 147446963, 147470372, 147003236, 145630217]}}, \"context\": {\"simple\": {\"62\": 1792147, \"49\": 48, \"24\": 1, \"25\": 147354771, \"27\": 596, \"22\": 63585, \"23\": 23, \"47\": 504183, \"42\": 0, \"29\": 17332, \"40\": 1788353, \"5\": 169, \"4\": 469613, \"7\": 18846, \"6\": 431260, \"9\": 26848, \"13\": 2, \"39\": 970, \"59\": 1275566, \"14\": 33331, \"17\": 48985, \"16\": 48811, \"19\": 52187,\"18\": 0, \"57\": 3102198867, \"56\": 1138207, \"37\": 1822268, \"35\": 31503, \"52\": 1,\"31\": 0}, \"clusters\": {\"33\": {\"615598\": 2, \"114743\": 1, \"669975\": 2, \"50327\": 0, \"10389746\": 5, \"55760\": 2, \"466541\": 6, \"32941670\": 1, \"292041\": 0, \"32975217\": 3, \"32942767\": 1, \"630543\": 5, \"1100\": 4, \"22939\": 7, \"124788\": 0, \"9422\": 0},\"51\": {\"1\": 255}, \"1\": {\"7\": 255}, \"46\": {\"472375\": 255, \"472376\": 255, \"472358\": 255}, \"3\": [50, 28, 34, 98, 28, 15], \"2\": [21, 11, 50, 74, 60, 23, 11]}, \"lists\": {\"11\": [1201425], \"8\": [18841, 18842], \"10\": [6, 13]}}, \"timestamp\": 1383778799938};";
}
