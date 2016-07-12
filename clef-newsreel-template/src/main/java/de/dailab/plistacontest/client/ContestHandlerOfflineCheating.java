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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.crowdrec.contest.evaluation.Impression;
import eu.crowdrec.contest.evaluation.LinkedFileCache;
import eu.crowdrec.contest.evaluation.LinkedFileCache.CacheEntry;


/**
 * Class to handle the communication with the CLEF NewsREEL challenge server Functions:
 * - parsing incoming HTTP-Requests - delegating requests according to their
 * types - responding to the plista challenge server
 * 
 * @author till, andreas, thanh
 * 
 */
public class ContestHandlerOfflineCheating extends AbstractHandler {

	/**
	 * Define the default logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContestHandlerOfflineCheating.class);

	/**
	 * here we store all relevant data about items.
	 */
	private final CheatingTable recommenderItemTable = new CheatingTable();


	/**
	 * Constructor, sets some default values.
	 * 
	 * @param _properties
	 * @param _contestRecommender
	 */
	public ContestHandlerOfflineCheating(final Properties _properties,	final String _groundTruthFilename, final long _desiredEvaluationTimespan) {

		try {
			System.out.println("init called");
			recommenderItemTable.initialize(_groundTruthFilename, _desiredEvaluationTimespan);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Handle incoming messages. This method is called by plista We check the
	 * message, and extract the relevant parameter values
	 * 
	 * @see org.eclipse.jetty.server.Handler#handle(java.lang.String,
	 *      org.eclipse.jetty.server.Request,
	 *      javax.servlet.http.HttpServletRequest,
	 *      javax.servlet.http.HttpServletResponse)
	 */
	public void handle(String arg0, Request _breq, HttpServletRequest _request,
			HttpServletResponse _response) throws IOException, ServletException {

		
		// we can only handle POST messages
		if (_breq.getMethod().equals("POST")) {

			if (_breq.getContentLength() < 0) {
				
				// handles first message from the server - returns OK
				logger.info("Initial Message with no content received.");
				response(_response, _breq, null, false);
				
			} else {

				// handle the normal messages - since we do not know the exact format, we try to be flexible
				String typeMessage = _breq.getParameter("type");
				String bodyMessage = _breq.getParameter("body");
				String propertyMessage = _breq.getParameter("properties");
				String entityMessage = _breq.getParameter("entities");

				String responseText = "";
				
				// handle idomaar messages
				if (bodyMessage == null || bodyMessage.length() == 0) {
				
					// we may recode the body message
					if (_breq.getContentType().equals("application/x-www-form-urlencoded; charset=utf-8")) {
						bodyMessage = URLDecoder.decode(bodyMessage,"utf-8");
						propertyMessage = URLDecoder.decode(propertyMessage,"utf-8");
						entityMessage = URLDecoder.decode(entityMessage, "utf-8");
					}
	
					// delegate the request and create a response message
					responseText =  handleIdomaarMessage(typeMessage, propertyMessage, entityMessage);
				} else {
					// handle old data format messages
					// we may recode the body message
					if (_breq.getContentType().equals("application/x-www-form-urlencoded; charset=utf-8")) {
						bodyMessage = URLDecoder.decode(bodyMessage,"utf-8");
					}
					responseText =  handleTraditionalMessage(typeMessage, bodyMessage);
				}
				// send the response message as text
				response(_response, _breq, responseText, true);
			}
		} else {
			// GET requests are answered by a HTML page
			logger.debug("Get request from " + _breq.getRemoteAddr());
			response(
					_response,
					_breq,
					"Visit <h3><a href=\"http://www.clef-newsreel.org/\">CLEF NewsREEL Challenge</a></h3>",
					true);
		}
	}

	/**
	 * Method to handle incoming messages from the server.
	 * 
	 * @param messageType
	 * 					the messageType of the incoming contest server message.
	 * @param properties
	 * 					
	 * @param entities
	 * @return the response to the contest server
	 */
	@SuppressWarnings("unchecked")
	private String handleIdomaarMessage(final String messageType, final String properties, final String entities) {
		// write all data from the server to a file
		// logger.info(messageType + "\t" + properties + "\t" + entities);

		// create an jSON object from the String
		final JSONObject jOP = (JSONObject) JSONValue.parse(properties);
		final JSONObject jOE = (JSONObject) JSONValue.parse(entities);
		
		// merge the different jsonObjects and correct missing itemIDs
		jOP.putAll(jOE);
		Object itemID = jOP.get("itemID");
		if (itemID == null) {
			jOP.put("itemID", 0);
		}

		// define a response object
		String response = null;

		if ("impression".equalsIgnoreCase(messageType)) {

			// parse the type of the event
			final RecommenderItem item = RecommenderItem.parseEventNotification(jOP.toJSONString());
			final String eventNotificationType = messageType; 

			// impression refers to articles read by the user
			if ("impression".equalsIgnoreCase(eventNotificationType)) {

				// we mark this information in the article table
				if (item.getItemID() != null) {

					response = "handle impression eventNotification successful";
					
					boolean recommendationExpected = false;
					if (properties.contains("\"event_type\": \"recommendation_request\"")) {
						recommendationExpected = true;
					}
					if (recommendationExpected) {
						List<Long> suggestedItemIDs = recommenderItemTable.getFutureItems(item.getTimeStamp(), item.getDomainID(), item.getUserID());
						response = "{" + "\"recs\": {" + "\"ints\": {" + "\"3\": " + suggestedItemIDs + "}" + "}}";
					}
					
				}
				// click refers to recommendations clicked by the user
			} else if ("click".equalsIgnoreCase(eventNotificationType)) {

				// we mark this information in the article table
				if (item.getItemID() != null) {

					response = "handle impression eventNotification successful";
				}
				response = "handle click eventNotification successful";

			} else {
				System.out.println("unknown event-type: "
						+ eventNotificationType + " (message ignored)");
			}

		} else if ("error_notification".equalsIgnoreCase(messageType)) {

			System.out.println("error-notification: " + jOP.toString() + jOE.toJSONString());

		} else {
			System.out.println("unknown MessageType: " + messageType);
			// Error handling
			logger.info(jOP.toString() + jOE.toJSONString());
			// this.contestRecommender.error(jObj.toString());
		}
		return response;
	}

	/**
	 * Method to handle incoming messages from the server.
	 * 
	 * @param messageType
	 *            the messageType of the incoming contest server message
	 * @param _jsonMessageBody
	 *            the incoming contest server message
	 * @return the response to the contest server
	 */
	@SuppressWarnings("unused")
	private String handleTraditionalMessage(final String messageType,
			final String _jsonMessageBody) {

		// write all data from the server to a file
		logger.info(messageType + "\t" + _jsonMessageBody);

		// create an jSON object from the String
		final JSONObject jObj = (JSONObject) JSONValue.parse(_jsonMessageBody);

		// define a response object
		String response = null;

		// TODO handle "item_create"

		// in a complex if/switch statement we handle the differentTypes of
		// messages
		if ("item_update".equalsIgnoreCase(messageType)) {

			// we extract itemID, domainID, text and the timeTime, create/update
			final RecommenderItem recommenderItem = RecommenderItem.parseItemUpdate(_jsonMessageBody);

			response = ";item_update successfull";
		}

		else if ("recommendation_request".equalsIgnoreCase(messageType)) {
            // THIS ISN'T WORKING BECAUSE REQUEST HAS A DIFFRENT JSON FORMAT THEN ITEM UPDATE
			//final RecommenderItem recommenderItem = RecommenderItem.parseItemUpdate(_jsonMessageBody);

			// we handle a recommendation request
			try {
				// parse the new recommender request
				RecommenderItem currentRequest = RecommenderItem.parseRecommendationRequest(_jsonMessageBody);

				// gather the items to be recommended
				List<Long> resultList = recommenderItemTable.getFutureItems(currentRequest.getTimeStamp(), currentRequest.getDomainID(), currentRequest.getUserID());
				if (resultList == null) {
					response = "[]";
					System.out.println("invalid resultList");
				} else {
					response = resultList.toString();
				}
				response = getRecommendationResultJSON(response);

				// TODO? might handle the the request as impressions
			} catch (Throwable t) {
				t.printStackTrace();
			}
		} else if ("event_notification".equalsIgnoreCase(messageType)) {

			// parse the type of the event
			final RecommenderItem item = RecommenderItem.parseEventNotification(_jsonMessageBody);
			final String eventNotificationType = item.getNotificationType();

			// impression refers to articles read by the user
			if ("impression".equalsIgnoreCase(eventNotificationType)
					|| "impression_empty"
							.equalsIgnoreCase(eventNotificationType)) {

				// we mark this information in the article table
				if (item.getItemID() != null) {
					response = "handle impression eventNotification successful";
				}
				// click refers to recommendations clicked by the user
			} else if ("click".equalsIgnoreCase(eventNotificationType)) {

				response = "handle click eventNotification successful";

			} else {
				System.out.println("unknown event-type: "
						+ eventNotificationType + " (message ignored)");
			}

		} else if ("error_notification".equalsIgnoreCase(messageType)) {

			System.out.println("error-notification: " + _jsonMessageBody);

		} else {
			System.out.println("unknown MessageType: " + messageType);
			// Error handling
			logger.info(jObj.toString());
			// this.contestRecommender.error(jObj.toString());
		}
		return response;
	}

	/**
	 * Response handler.
	 * 
	 * @param _response
	 *            {@link HttpServletResponse} object
	 * @param _breq
	 *            the initial request
	 * @param _text
	 *            response text
	 * @param _b
	 *            boolean to set whether the response text should be sent
	 * @throws IOException
	 */
	private void response(HttpServletResponse _response, Request _breq,	String _text, boolean _b) throws IOException {
		
		// configure the esponse parameters
		_response.setContentType("text/html;charset=utf-8");
		_response.setStatus(HttpServletResponse.SC_OK);
		_breq.setHandled(true);

		if (_text != null && _b) {
			_response.getWriter().println(_text);
			if (_text != null && !_text.startsWith("handle")) {
				logger.debug("send response: " + _text);
			}
		}
	}

	/**
	 * Create a json response object for recommendation requests.
	 * 
	 * @param _itemsIDs
	 *     the recommendation result
	 * @return 
	 * 		the recommendation result as json
	 */
	public static final String getRecommendationResultJSON(String _itemsIDs) {

		// invalid recommendations result in empty result sets
		if (_itemsIDs == null || _itemsIDs.length() == 0) {
			_itemsIDs = "[]";
		} 
		// add brackets if needed
		else if (!_itemsIDs.trim().startsWith("[")) {
			_itemsIDs = "[" + _itemsIDs + "]";
		}
		// build result as JSON according to formal requirements
		String result = "{" + "\"recs\": {" + "\"ints\": {" + "\"3\": "
				+ _itemsIDs + "}" + "}}";

		return result;
	}
	
	
	public static class CheatingTable {
		private Map<String, LinkedList<LinkedFileCache.CacheEntry>> recommenderItemTable = new HashMap<String, LinkedList<LinkedFileCache.CacheEntry>>();
		private BufferedReader brImpresssions = null;
		private long desiredEvaluationTimespan = 0;
		
		public boolean initialize(final String _groundTruthFilename, final long _desiredEvaluationTimespan) {
			this.desiredEvaluationTimespan = _desiredEvaluationTimespan;
			try {
				File groundTruthFile = new File(_groundTruthFilename);
				InputStream is;
				if (groundTruthFile.isFile()) {
					is = new FileInputStream(_groundTruthFilename);
					// support gZip files
					if (groundTruthFile.getName().toLowerCase().endsWith(".gz")) {
						is = new GZIPInputStream(is);
					}
				}
				else {
					// if the input is a directory, consider all files based on a pattern
					File[] childs = groundTruthFile.listFiles(new FilenameFilter() {
						
						@Override
						public boolean accept(File dir, String name) {
							final String fileName = name.toLowerCase();
							return fileName.endsWith("data.idomaar.txt.gz") || fileName.endsWith("data.idomaar.txt") ||
                                    fileName.endsWith(".data.gz") ||
                                    (fileName.startsWith("contest.log.") && fileName.endsWith(".gz"));
						}
					});
					if (childs == null || childs.length == 0) {
						throw new IOException("invalid inLogFileName or empty directory");
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
				brImpresssions = new BufferedReader(new InputStreamReader(is));
				return true;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		
		private List<Long> getFutureItems(final long _timeStamp, final long _domainID, final long _userID) {
			
			synchronized (CheatingTable.class) {
				
			List<Long> result = new ArrayList<Long>(6);
			
			try { // catch all exceptions
				
				// adapt the buffer for the current timeStamp
				adaptBufferForNewTimeStamp(_timeStamp);
				
				// 
				LinkedList<LinkedFileCache.CacheEntry> listForTheActiveUser = recommenderItemTable.get(_userID + "|" + _domainID);
				
				// if we do not know something about the user, we return an empty list: we just do not know what to recommender.
				if (listForTheActiveUser == null || listForTheActiveUser.size() == 0) {
					return result;
				}
				

				while (!listForTheActiveUser.isEmpty()) {
					LinkedFileCache.CacheEntry entry = listForTheActiveUser.removeFirst();
					
					// ignore invalid itemIDs
					if (entry.getItemID() == null || entry.getItemID().equals(0L)) {
						continue;
					}
					
					// copy valid IDs into the result list
					result.add(entry.getItemID());
					
					// return up to 6 results;
					if (result.size() >= 6) {
						break; 
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return result;
		}
		}
		/**
		 * Retrieve a new line from the file and refill the cache.
		 * @return success 
		 * @throws Exception probably a fileHandling problem.
		 */
		private boolean adaptBufferForNewTimeStamp(final long _timeStamp) throws Exception{
			
			// for all keys in the table, remove the out-dated entries, find the max TimeStamp
			long newestTimeStamp = _timeStamp;
			Set<Map.Entry<String, LinkedList<LinkedFileCache.CacheEntry>>> entrySet = recommenderItemTable.entrySet();
			Set<Map.Entry<String, LinkedList<CacheEntry>>> tmpEntrySet = new HashSet<Map.Entry<String,LinkedList<CacheEntry>>>();
			tmpEntrySet.addAll(entrySet);
			for (Map.Entry<String, LinkedList<CacheEntry>> entry : tmpEntrySet) {
				LinkedList<LinkedFileCache.CacheEntry> list = entry.getValue();
				if (!list.isEmpty()) {
					Long oldestTimeStamp = list.getFirst().getTimeStamp();
					
					while (oldestTimeStamp < _timeStamp) {
						list.removeFirst();
						if (list.isEmpty()) {
							recommenderItemTable.remove(entry.getKey());
							break;
						}
						oldestTimeStamp = list.getFirst().getTimeStamp();
					}
				}
				
				// having adapted the list, we store the max timeStamp over all entries
				if (!list.isEmpty()) {
					newestTimeStamp = Math.max(newestTimeStamp, list.getLast().getTimeStamp());
				}
			}

			// fill the buffer with additional lines from the buffer
			boolean moreLinesToRead = true;
			long requestedNewestTimeStamp = _timeStamp  + desiredEvaluationTimespan;
			while (newestTimeStamp < requestedNewestTimeStamp && moreLinesToRead) {

				// read the newest line
				String line = brImpresssions.readLine();
				
				// if end of file is reached
				if (line == null) {
					moreLinesToRead = false;
					System.out.println("read everything");
					return false;
				}
				
				// ignore comments
				if (line.startsWith("#")) {
					continue;
				}


				
				// parse the new line and add the line to the cache
				Impression impression = Impression.createImpressionFrom4CollCSV(line);
                if(impression == null) {
                    continue;
                }
				CacheEntry ce = new CacheEntry(impression.getUserID(), impression.getItemID(), impression.getDomainID(), impression.getTimeStamp());
				newestTimeStamp = ce.getTimeStamp();
				
				// put the key data extracted from the line in the cache
				String key = ce.getUserID() + "|" + ce.getDomainID();
				
				if (!recommenderItemTable.containsKey(key)) {
					recommenderItemTable.put(key, new LinkedList<LinkedFileCache.CacheEntry>());
				}
				recommenderItemTable.get(key).add(ce);
			}
			return true;	
		}
	}
}
