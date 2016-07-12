/*
Copyright (c) 2014, TU Berlin
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

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Properties;

/**
 * Class to handle the communication with the CLEF NewsREEL challenge server Functions:
 * - parsing incoming Requests (based on 0MQ) - parse and handle the messages
 * 
 * @author till, andreas, thanh
 * 
 */
public class ClientAndContestHandler0MQ {

	/**
	 * Define the default logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientAndContestHandler0MQ.class);

	/**
	 * here we store all relevant data about items.
	 */
	private final RecommenderItemTable recommenderItemTable = new RecommenderItemTable();

	/**
	 * Define the default recommender, currently not used.
	 */
	@SuppressWarnings("unused")
	private Object contestRecommender;

	/**
	 * Constructor, sets some default values.
	 * 
	 * @param _properties
	 * @param _contestRecommender
	 */
	public ClientAndContestHandler0MQ(final Properties _properties,
			final Object _contestRecommender) {

		this.contestRecommender = _contestRecommender;

	}

	/**
	 * Handle incoming messages. This method is called by plista We check the
	 * message, and extract the relevant parameter values
	 * 
	 * @see org.eclipse.jetty.server.Handler#handle(String,
	 *      org.eclipse.jetty.server.Request,
	 *      javax.servlet.http.HttpServletRequest,
	 *      javax.servlet.http.HttpServletResponse)
	 */
	public String handle(String typeMessage, String bodyMessage, String propertyMessage, String entityMessage) throws IOException {

		String responseText = "";
		String encoding = null;

		// handle idomaar messages
		if (bodyMessage == null || bodyMessage.length() == 0) {

			// we may recode the body message
			if (encoding != null) {
				bodyMessage = URLDecoder.decode(bodyMessage, encoding);
				propertyMessage = URLDecoder.decode(propertyMessage, encoding);
				entityMessage = URLDecoder.decode(entityMessage, encoding);
			}

			// delegate the request and create a response message
			responseText = handleIdomaarMessage(typeMessage, propertyMessage, entityMessage);
			return responseText;

		} else {
			// handle old data format messages
			// we may recode the body message
			if (encoding != null) {
				bodyMessage = URLDecoder.decode(bodyMessage, "utf-8");
			}
			responseText = handleTraditionalMessage(typeMessage, bodyMessage);
		}
		// send the response message as text
		return responseText;
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
	private String handleIdomaarMessage(final String messageType, final String properties, final String entities) {
		// write all data from the server to a file
		logger.info(messageType + "\t" + properties + "\t" + entities);

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
			final String eventNotificationType = item.getNotificationType(); 

			
			// new items shall be added to the list of items
			if (item.getItemID() != null) {
				recommenderItemTable.handleItemUpdate(item);
			}
			response = "handle impression eventNotification successful";
			
			// impression refers to articles read by the user
			if ("recommendation_request".equalsIgnoreCase(eventNotificationType)) {

				// we mark this information in the article table
				if (item.getItemID() != null) {

					item.setNumberOfRequestedResults(6);
					//List<Long> suggestedItemIDs = item.getListOfDisplayedRecs();
					List<Long> suggestedItemIDs = recommenderItemTable.getLastItems(item);
					response = "{" + "\"recs\": {" + "\"ints\": {" + "\"3\": " + suggestedItemIDs + "}" + "}}";
					
				} else {
					System.err.println("invalid itemID - requests are only valid for 'normal' articles");
				}
				// click refers to recommendations clicked by the user
			} else if ("click".equalsIgnoreCase(eventNotificationType)) {

				// we mark this information in the article table
				if (item.getItemID() != null) {
					// new items shall be added to the list of items
					recommenderItemTable.handleItemUpdate(item);

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
	 * @param _jsonString
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
			final RecommenderItem recommenderItem = RecommenderItem
					.parseItemUpdate(_jsonMessageBody);

			// we mark this information in the article table
			if (recommenderItem.getItemID() != null) {
				recommenderItemTable.handleItemUpdate(recommenderItem);
			}

			response = ";item_update successfull";
		}

		else if ("recommendation_request".equalsIgnoreCase(messageType)) {
			final RecommenderItem recommenderItem = RecommenderItem
					.parseItemUpdate(_jsonMessageBody);

			// we handle a recommendation request
			try {
				// parse the new recommender request
				RecommenderItem currentRequest = RecommenderItem
						.parseRecommendationRequest(_jsonMessageBody);

				// gather the items to be recommended
				List<Long> resultList = recommenderItemTable
						.getLastItems(currentRequest);
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
					// new items shall be added to the list of items
					recommenderItemTable.handleItemUpdate(item);

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

	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////////////////////////////////////
	///////////////////////////////////////////////
	
	/**
	 * the default constructor.
	 */
	public ClientAndContestHandler0MQ() {
		super();
	}

	/**
	 * This method starts the server
	 * 
	 * @param args [hostname:port, properties_filename]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// you might want to use a recommender
		Object recommender = null;

		String hostname = "0.0.0.0";
		int port = 8081;
		try {
			hostname = args[0].substring(0, args[0].indexOf(":"));
			port = Integer.parseInt(args[0].substring(args[0].indexOf(":") + 1));
		} catch (Exception e) {
			System.out.println("No hostname and port given. Using default 0.0.0.0:8081");
			logger.info(e.getMessage());
		}
		
		// initialize zeroMQ
		// initialize the 0MQ
		ZContext context = new ZContext();
		ZMQ.Socket clientSocket = context.createSocket(ZMQ.SUB);
		String serverURL = "tcp://" + hostname + ":" + port;
		if (serverURL == null) {
			serverURL = "tcp://127.0.0.1:8088";
		}
		clientSocket.bind(serverURL);
		
		// create the handler 
		ClientAndContestHandler0MQ me = new ClientAndContestHandler0MQ();
		for(;;) {
			String message = clientSocket.recvStr();
			// split the message
			String[] token = message.split("\t");

			// call handle and provide the relevant token
			String result = me.handle(token[0], null, token[3], token[4]);
			
			// send back the result
			clientSocket.send(result);			
		}
	}
}
