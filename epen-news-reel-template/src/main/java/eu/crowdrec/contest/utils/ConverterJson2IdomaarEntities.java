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
public class ConverterJson2IdomaarEntities {

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

					// check the parsed object
					try {
						jsonObj.get("timestamp");
					} catch (Exception e) {
						System.err.println("error handling the json object.");
						System.err.println("messageBody=" + messageBody);
						System.err.println("inputline=" + line);
						System.err.println("exception=" + e);
						System.err.println("ignoring the line (" + count + ")");
						continue;
					}
					
					// extract the timeStamp
					// if the timeStamp is null, use updated_at
					// if updated_at is null, use created_at
					Object timeStampJSON = jsonObj.get("timestamp");
					if (timeStampJSON == null) {
						timeStampJSON = jsonObj.get("updated_at");
					}
					if (timeStampJSON == null) {
						timeStampJSON = jsonObj.get("created_at");
					}


					// extract the messageType
					messageType = "unknown";
					Object messageTypeJSON = jsonObj.get("version");
					if (messageTypeJSON != null) {
						if ("1".equalsIgnoreCase(messageTypeJSON+"")) {
							messageType = "item-created";
						} else {
							messageType = "item-updated";
						}
					} else {
						System.err.println("messageType could not be extracted = " + messageTypeJSON);
					}
										
					// extract the itemID
					String itemID = jsonObj.get("id") + "";	

					// extract the itemID
					String domainID = jsonObj.get("domainid") + "";
		
					
					// write output
					bw.write(messageType + separator);
					bw.write(itemID + separator);
					bw.write(timeStampJSON + separator);
					bw.write(messageBody+ separator);
					bw.write("{\"itemID\":" + itemID + ", \"domainID\":" + domainID + "}" + separator);
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
			System.out.println("usage: java ConverterJson2IdomaarEntities sourceFileName [targetFileName].");
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
}
