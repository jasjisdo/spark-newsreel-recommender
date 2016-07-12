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

package eu.crowdrec.contest.sender;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class simulates an online evaluation by re-playing an previously recorded stream.
 * 
 * The request sender reads line by line an input file and sends each line to a recommender server.
 * The recommendation responses are collected and stored in an output file, optimized for the evaluator.
 * The lines for the evaluator contain the most ID of the request and the response of the recommender service.
 * If an output file for the evaluator is created, the sender must parse the input file.
 * Currently raw CLEF newsREEL files as well as idomaar data files are supported.
 * 
 * 
 * @author andreas
 * @author thanh
 */

public class RequestSender {


	/** the methods used for creating http connections */
	public static enum HttpLib {
		HTTPCLIENT, JAVA
	}
	
	/** define the library used for creating a http connection */
	public static HttpLib httpLib = HttpLib.HTTPCLIENT; 
	
	/** the httpClient instance used for creating the connection */
	private final static HttpConnectionManager httpConnectionManager = new MultiThreadedHttpConnectionManager();
	static {
		// configure the http client - httpclient should run in a multi-threaded environment
		final HttpConnectionManagerParams httpConnectionManagerParams = new HttpConnectionManagerParams();
		httpConnectionManagerParams.setDefaultMaxConnectionsPerHost(100);
		httpConnectionManagerParams.setMaxTotalConnections(100);
		httpConnectionManager.setParams(httpConnectionManagerParams);
	}
	final static HttpClient httpClient = new HttpClient(httpConnectionManager);
	
	/**
	 * should a thread pool be used when sending requests
	 */
	public static boolean useThreadPool = true;
	
	/**
	 * the default logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(RequestSender.class);
	
	
	/**
	 * count the number of lines
	 */
	private static volatile long numberOfInputLines = 0L;
	private static volatile long numberOfOutputLines = 0L;

	/**
	 * count active threads
	 */
	private static AtomicInteger threadcount = new AtomicInteger(0);

	/**
	 * limit the amount of active threads
	 */
	private static Integer MAX_THREADS = 1000;


	/**
	 * Send a line from a logFile to an HTTP server (single-threaded).
	 * 
	 * @param logline the line that should by sent
	 * 
	 * @return the response or null (if an error has been detected)
	 */
	static private String excutePost(final String logline, final String serverURL) {
		
		// split the logLine into several token
		String[] token = logline.split("\t");

		String type = token[0];
		String property = token[3];
		String entity = token[4];
		
		// encode the content as URL parameters.
		String urlParameters = "";
		try {
			urlParameters = String.format("type=%s&properties=%s&entities=%s",
					URLEncoder.encode(type, "UTF-8"),
					URLEncoder.encode(property, "UTF-8"),
					URLEncoder.encode(entity, "UTF-8"));
		} catch (UnsupportedEncodingException e1) {
			logger.warn(e1.toString());
		}

		// initialize a HTTP connection to the server
		// reuse of connection objects is delegated to the JVM
		HttpURLConnection connection = null;
		
		try {
			final URL url = new URL(serverURL);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			connection.setRequestProperty("Content-Language", "en-US");
			connection.setUseCaches(false);
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setRequestProperty("Content-Length",	"" + Integer.toString(urlParameters.getBytes().length));

			// Send request
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();

			// Get Response
			BufferedReader rd = null;
			InputStream is = null;
			try {
				is = connection.getInputStream();
				rd = new BufferedReader(new InputStreamReader(is));
				
				StringBuffer response = new StringBuffer();
				for (String line = rd.readLine(); line != null; line = rd.readLine()) {
					response.append(line);
					response.append(" ");
				}
				return response.toString();
			} catch (IOException e) {
				logger.warn("receiving response failed, ignored.");
			}
			finally {
				if (is != null) {
					is.close();
				}
				if (rd != null) {
					rd.close();
				}
			}
		} catch (MalformedURLException e) {
			System.err.println("invalid server URL, program stopped.");
			System.exit(-1);
		} catch (IOException e) {
			System.err.println("i/o error connecting the http server, program stopped. e=" + e);
			System.exit(-1);
		} catch (Exception e) {
			System.err.println("general error connecting the http server, program stopped. e:" + e);
			e.printStackTrace();
			System.exit(-1);
		} finally {
			// close the connection
			if (connection != null) {
				connection.disconnect();
			}
		}
		return null;
	}
	
	/**
	 * Send a line from a logFile to an HTTP server.
	 * 
	 * @param logline the line that should by sent
	 * 
	 * @return the response or null (if an error has been detected)
	 */
	static private String excutePostWithHttpClient(final String logline, final String serverURL) {
		
		// split the logLine into several token
		String[] token = logline.split("\t");

		// define the URL parameter
		String urlParameters = "";
		
		boolean oldMethod = token.length < 4;
		if (oldMethod) {
			try {
				String type = logline.contains("\"event_type\": \"recommendation_request\"")
					? "recommendation_request"
					: "event_notification";
				String newLine = logline.substring(0, logline.length()-1) + ", \"limit\":6, \"type\":\"impression\"}";
				urlParameters = String.format("type=%s&body=%s",
						URLEncoder.encode(type, "UTF-8"),
						URLEncoder.encode(newLine, "UTF-8"));

			} catch (UnsupportedEncodingException e1) {
				logger.warn(e1.toString());
			}			
		} else {
			String type = token[0];
			String property = token[3];
			String entity = token[4];
			
			// encode the content as URL parameters.
			try {
				urlParameters = String.format("type=%s&properties=%s&entities=%s",
						URLEncoder.encode(type, "UTF-8"),
						URLEncoder.encode(property, "UTF-8"),
						URLEncoder.encode(entity, "UTF-8"));
			} catch (UnsupportedEncodingException e1) {
				logger.warn(e1.toString());
			}
		}

		PostMethod postMethod = null;
		try {
			StringRequestEntity requestEntity = new StringRequestEntity(
					urlParameters, "application/x-www-form-urlencoded", "UTF-8");

			postMethod = new PostMethod(serverURL);
			postMethod.setParameter("useCache", "false");
			postMethod.setRequestEntity(requestEntity);

			int statusCode = httpClient.executeMethod(postMethod);
			String response = 
				statusCode == 200
					? postMethod.getResponseBodyAsString()
					: "statusCode:" + statusCode;

			return response.trim();
		} catch (IOException e) {
			logger.warn("receiving response failed, ignored.");
		} finally {
			if (postMethod != null) {
				postMethod.releaseConnection();
			}
		}
		return null;
	}

	/**
	 * read logFile then sends line by line to server.
	 * 
	 * @param inLogFileName
	 *            path to log file. That can be a zip file or text file.
	 * @param outLogFile
	 *            path to outLog file. The outLog file should be analyzed by the evaluator.
	 *            if the filename is null; no output will be generated
	 * @param serverURL
	 *            URL of the server
	 */
	public static void sender(final String inLogFileName, final String outLogFile, final String serverURL) {
		
		// handle the log file
		// check type of file
		
		// try to read the defined logFile
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			// if outLogFile name is not null, create an output file
			if (outLogFile != null && outLogFile.length() > 0) {
				bw = new BufferedWriter(new FileWriter(new File(outLogFile), false));
			}
			
			// support a list of files in a directory
			File inLogFile = new File(inLogFileName);
			InputStream is;
			if (inLogFile.isFile()) {
				is = new FileInputStream(inLogFileName);
				// support gZip files
				if (inLogFile.getName().toLowerCase().endsWith(".gz")) {
					is = new GZIPInputStream(is);
				}
			}
			else {
				// if the input is a directory, consider all files based on a pattern
				File[] childs = inLogFile.listFiles(new FilenameFilter() {
					
					@Override
					public boolean accept(File dir, String name) {
						final String fileName = name.toLowerCase();
						return fileName.endsWith("data.idomaar.txt.gz") || fileName.endsWith("data.idomaar.txt") || fileName.endsWith(".data.gz") || fileName.endsWith(".data");
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
			
			
			// read the log file line by line
			br = new BufferedReader(new InputStreamReader(is));
			try {
				for (String line = br.readLine(); line != null; line = br.readLine()) {
					
					// ignore invalid lines and header
					if (line.startsWith("null") || line.startsWith("#")) {
						continue;
					}
					numberOfInputLines++;

					RequestSenderThread t = new RequestSenderThread(line, serverURL, bw);
					threadcount.incrementAndGet();
					t.start();

					while (threadcount.get() >= MAX_THREADS) {
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
							logger.error("failed pend for open thread slots", e);
						}
					}
				}
			} catch (IOException e) {
				logger.warn(e.toString(), e);
			}
		} catch (FileNotFoundException e) {
			logger.error("logFile not found e:" + e.toString());
		} catch (IOException e) {
			logger.error("reading the logFile failed e:" + e.toString());
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.debug("close read-log file failed");
				}
			}
			if (bw != null) {
				try {
					// wait for ensuring that all request are finished
					// this simplifies the management of thread and worked fine for all test machines
					Thread.sleep(5000);
					bw.flush();
				} catch (Exception e) {
					logger.debug("close write-log file failed");
				}
			}
		}
	}


	
	
	/**
	 * Start the logFile sender.
	 * 
	 * @param args
	 *            String[]{hostname:port, inLogfileName, outLogFileName, max_threads}
	 */
	public static void main(String[] args) {
		
		if (args.length == 2 || args.length == 3 || args.length == 4) {
			String args2 = args.length >= 3 ? args[2] : null;
			if (args.length == 4)
				try {
					MAX_THREADS = Integer.parseInt(args[3]);
				} catch (Exception e) {
					System.err.println("failed to parse integer "+args[3]);
				}
			long startTime = System.currentTimeMillis();
			sender(args[1], args2, args[0]);
			long endTime = System.currentTimeMillis();
			System.out.println("finished (threadPool=" + useThreadPool + "): " + (endTime - startTime) + " (Finished: " + new Date() + ")");
			System.out.println("throughput - messages per second: " + (1000L * numberOfInputLines / (endTime - startTime)));
			System.out.println("throughput - requests per second: " + (1000L * numberOfOutputLines / (endTime - startTime)));
			System.out.println("numberOfInputLines = " + numberOfInputLines);
			System.out.println("numberOfOutputLines = " + numberOfOutputLines);
			System.out.println("MAX_THREADS = " + MAX_THREADS);
			try {
				Thread.sleep(5000);
			} catch (Exception e) {
			}
		} else {
			System.err.println("wrong number of parameters.");
			System.err.println("usage: java RequestSender <hostName>:<port> <logfileName>");
			System.err.println(".gz files are supported.");
		}
	}
	
	/**
	 * A class for sending messages concurrently
	 * @author andreas
	 *
	 */
	public static class RequestSenderThread extends Thread {

		// member variables
		final String line;
		final String serverURL;
		final BufferedWriter bw;
		
		/**
		 * Constructor
		 * @param line
		 * @param serverURL
		 * @param bw
		 */
		public RequestSenderThread(final String line, final String serverURL, final BufferedWriter bw) {
			this.line = line;
			this.serverURL = serverURL;
			this.bw = bw;
		}
		
		/**
		 * Execute a HTTP post request based on a line from the input file.
		 * If in output file is provided, parse the input line and create a line for the evaluator.
		 * The file writing is synchronized. Since file writing is done after having received the response, the file writing should not be the bottleneck, but it may reorder the lines in the output file.
		 * 
		 * @see java.lang.Thread#run()
		 */
		@Override
		public void run() {
			
			final long startTime = System.currentTimeMillis();
			
			// select the lib and send the http request
			String result = 
				HttpLib.HTTPCLIENT.equals(httpLib)
				? excutePostWithHttpClient(line, serverURL)
				: excutePost(line, serverURL);

			long responseTime = System.currentTimeMillis() - startTime;

			threadcount.decrementAndGet();

			// if the output file is not null, write the output in a synchronized way
			boolean answerExpected = false;
			if (line.contains("\"event_type\": \"recommendation_request\"")) {
				answerExpected = true;
			}
			
			if (answerExpected) {
				if (logger.isInfoEnabled()) {
					logger.info("serverResponse: " + result);
				}
				numberOfOutputLines++;
				
				// if the output file is not null, write the output in a synchronized way
				if (bw != null) {
					String[] data = LogFileUtils.extractEvaluationRelevantDataFromInputLine(line);
					String requestId = data[0];
					String userId = data[1];
					String itemId = data[2];
					String domainId = data[3];
					String timeStamp = data[4];
					synchronized (bw) {
						try {
							bw.write("prediction\t" + requestId + "\t" + timeStamp + "\t" + responseTime + "\t" + itemId+ "\t" + userId + "\t" + domainId + "\t" + result + "\t" + (data.length>5?data[5]:""));
							bw.newLine();
						}catch (Exception e) {
							logger.warn(e.toString(), e);
						}
					}
				}
			}
		}
	}
}
