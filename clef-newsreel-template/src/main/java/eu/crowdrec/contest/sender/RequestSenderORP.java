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

import java.io.*;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 */

public class RequestSenderORP {
	
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
	
	private static final Pattern logLinePattern = Pattern.compile("([A-z]*)\\t(.*)\\,(.{23})$");
	private static final SimpleDateFormat sdfLogDate = new SimpleDateFormat("yyyy'-'MM'-'dd' 'HH':'mm':'ss','SSS");
	private static int resultLineCount = 0;
	/**
	 * the default logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(RequestSenderORP.class);



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
	 * Send a line from a logFile to an HTTP server.
	 * 
	 * @param type the line that should by sent
	 * 
	 * @param body the connection to the http server, must not be null
     *
     * @param serverURL the connection to the http server, must not be null
	 * 
	 * @return the response or null (if an error has been detected)
	 */
	static private String excutePostWithHttpClient(final String type, final String body, final String serverURL) {
		
		// define the URL parameter
		String urlParameters = "";
		
		try {

			urlParameters = String.format("type=%s&body=%s",
					URLEncoder.encode(type, "UTF-8"),
					URLEncoder.encode(body, "UTF-8"));

		} catch (UnsupportedEncodingException e1) {
			logger.warn(e1.toString());
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
	 * @param outLogFileName
	 *            path to outLog file. The outLog file should be analyzed by the evaluator.
	 *            if the filename is null; no output will be generated
	 * @param serverURL
	 *            URL of the server
	 */
	public static void sender(final String inLogFileName, final String outLogFileName, final String serverURL) {

		// handle the log file
        File inLogFile = null;
        if(inLogFileName != null && inLogFileName != "") {
            inLogFile = new File(inLogFileName);
            logger.info("location inLogFile: " + inLogFile.getAbsolutePath());
            logger.info("inLogFile exists?: " + inLogFile.exists());
            logger.info("inLogFile folder canRead?: " + inLogFile.canRead());
        } else {
            logger.info("location inLogFile is null or empty!");
        }
        File outLogFile = null;
        if(outLogFileName != null && outLogFileName != "") {
            outLogFile = new File(outLogFileName);
            logger.info("location outlogf: " + outLogFile.getAbsolutePath());
            logger.info("outLogFile exists?: " + outLogFile.exists());
            logger.info("outLogFile folder canWrite?: " + outLogFile.getParentFile().canWrite());
        } else {
            logger.info("location outLogFile is null or empty!");
        }

		// check type of file
		
		// try to read the defined logFile
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
            // if outLogFile is not null
            if(outLogFile != null) {
                // check write permissions of outLogFile parent folder.
                if(outLogFile.getParentFile().canWrite()) {
                    // create outLogFile if not exists!
                    if(!outLogFile.exists()) {
                        outLogFile.createNewFile();
                        logger.info("outLogFile created: " + outLogFile.exists());
                    }
                    if (outLogFile.exists() && outLogFile.canWrite()) { // if outLogFile exists and is writeable.
                        // bw = new BufferedWriter(new FileWriter(new File(outLogFileName), false)); // write in file

                        // write in file (platform independent in UTF-8)
                        FileOutputStream fileOutStream = new FileOutputStream(outLogFileName, false);
                        bw = new BufferedWriter(new OutputStreamWriter(fileOutStream, "UTF-8"));

                    }
                } else {
                    logger.error("no write permissions in parent folder of outLogFile!");
                    return;
                }
            } else {
                logger.error("outLogFile location is null!");
                return;
            }
			
			// support a list of files in a directory
			inLogFile = new File(inLogFileName);
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
						return fileName.startsWith("contest.log");
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
					
					String[] token = parseLogLine(line);
					if (token == null) {
						System.err.println("HHHHHHHHHHHHHHHHHHHHHHHHHHEEEEEEEEEEEEEEEEEEELLLLLLLLLLLLLLLLPPPPPPPPPPPPPPP");
						System.err.println("HHHHHHHHHHHHHHHHHHHHHHHHHHEEEEEEEEEEEEEEEEEEELLLLLLLLLLLLLLLLPPPPPPPPPPPPPPP");
						System.err.println("HHHHHHHHHHHHHHHHHHHHHHHHHHEEEEEEEEEEEEEEEEEEELLLLLLLLLLLLLLLLPPPPPPPPPPPPPPP");
						System.err.println("HHHHHHHHHHHHHHHHHHHHHHHHHHEEEEEEEEEEEEEEEEEEELLLLLLLLLLLLLLLLPPPPPPPPPPPPPPP");
						System.err.println("HHHHHHHHHHHHHHHHHHHHHHHHHHEEEEEEEEEEEEEEEEEEELLLLLLLLLLLLLLLLPPPPPPPPPPPPPPP");
						System.err.println("HHHHHHHHHHHHHHHHHHHHHHHHHHEEEEEEEEEEEEEEEEEEELLLLLLLLLLLLLLLLPPPPPPPPPPPPPPP");
						System.err.println("HHHHHHHHHHHHHHHHHHHHHHHHHHEEEEEEEEEEEEEEEEEEELLLLLLLLLLLLLLLLPPPPPPPPPPPPPPP");
						continue;
					}

					numberOfInputLines++;

					RequestSenderThread t = new RequestSenderThread(token[0], token[1], token[2], serverURL, bw);
					threadcount.incrementAndGet();
					t.start();

					while (threadcount.get() >= MAX_THREADS) {
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
							logger.error("failed pend for open thread slots", e);
						}
					}
					
					/*// use a threadPool
					RequestSenderThread t = new RequestSenderThread(token[0], token[1], token[2], serverURL, bw);
					try {
						// try to limit the speed of sending requests
						if (Thread.activeCount() > 1000) {
							if (logger.isDebugEnabled()) {
								logger.debug("Thread.activeCount() = " + Thread.activeCount());
							}
							Thread.sleep(200);
						}
					} catch (Exception e) {
						logger.info(e.toString());
					}
					t.start();*/
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

	public static String[] parseLogLine(final String logLine) {
		
		Matcher m = logLinePattern.matcher(logLine);
	      if (m.find( )) {
	         //System.out.println("0: " + m.group(1) );
	         //System.out.println("1: " + m.group(2) );
	         //System.out.println("2: " + m.group(3) );
	    	  return new String[]{m.group(1), m.group(2), m.group(3)};
	      } else {
	         //System.out.println("NO MATCH");
	    	  return null;
	      }
	      
	}

	/**
	 * Start the logFile sender.
	 * 
	 * @param args
	 *            String[]{hostname:port, inLogfileName, outLogFileName}
	 */
	public static void main(String... args) throws Exception {

        logger.info("args.len="+args.length);

		if (args.length == 2 || args.length == 3) {
			String args2 = args.length == 3 ? args[2] : null;
			logger.info("arg0="+args[0]);
			logger.info("arg1="+args[1]);
			logger.info("arg2="+args[2]);

			long startTime = System.currentTimeMillis();

			sender(args[1], args2, args[0]);

			long endTime = System.currentTimeMillis();
			try {
				Thread.sleep(5000);
			} catch (Exception e) {
                e.printStackTrace();
			}
			System.out.println("finished: " + (System.currentTimeMillis() - startTime) + " (Finished: " + new Date() + ")");
            File outLogFile = new File(args2);
            logger.info("result file location: " + outLogFile.getAbsolutePath());
            logger.info("result file exist: " + outLogFile.exists());
			System.out.println("finished: " + (endTime - startTime) + " (Finished: " + new Date() + ")");
			System.out.println("throughput - messages per second: " + (1000L * numberOfInputLines / (endTime - startTime)));
			System.out.println("throughput - requests per second: " + (1000L * numberOfOutputLines / (endTime - startTime)));
			System.out.println("numberOfInputLines = " + numberOfInputLines);
			System.out.println("numberOfOutputLines = " + numberOfOutputLines);
			System.out.println("MAX_THREADS = " + MAX_THREADS);
		} else {
			System.err.println("wrong number of parameters.");
			System.err.println("usage: java RequestSenderORP <hostName>:<port> <logfileName>");
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
		final String messageType;
		final String messageBody;
		final String messageTimestamp;
		final String serverURL;
		final BufferedWriter bw;
		
		/**
		 * Constructor
		 * @param messageType
		 * @param serverURL
		 * @param bw
		 */
		public RequestSenderThread(final String messageType, final String messageBody, final String messageTimestamp, final String serverURL, final BufferedWriter bw) {
			this.messageType = messageType;
			this.messageTimestamp = messageTimestamp;
			this.serverURL = serverURL;
			this.bw = bw;

            if("recommendation_request".equalsIgnoreCase(messageType)) {
				if(messageTimestamp != null && !messageTimestamp.isEmpty()) {
					this.messageBody = injectTimeStampToMessageBody(messageBody, messageTimestamp);
				} else {
					this.messageBody = messageBody;
				}
            } else {
                this.messageBody = messageBody;
            }

		}

        /*
         * Inject a JSON object '"timestamp": {Long}' in message body of recomandation request.
         * @param messageBody body of recomandation request (in JSON structure)
         * @param messageTimestamp
         * @return A message body with injected timestamp
         */
        private String injectTimeStampToMessageBody(String messageBody, String messageTimestamp) {

            Date timestampAsDate = null;
            try {
                timestampAsDate = sdfLogDate.parse(messageTimestamp);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            StringBuilder sb = new StringBuilder();
            sb.append(messageBody);
            sb.deleteCharAt(sb.length() - 1);
            sb.append(",\"timestamp\":");
            sb.append(timestampAsDate.getTime());
            sb.append("}");
            return sb.toString();
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
			String result = excutePostWithHttpClient(this.messageType, this.messageBody, serverURL);
			
			// if the output file is not null, write the output in a synchronized way
			boolean answerExpected = false;
			if ("recommendation_request".equalsIgnoreCase(this.messageType)) {
				answerExpected = true;
			}

			long responseTime = System.currentTimeMillis() - startTime;

			threadcount.decrementAndGet();

			if (answerExpected) {
				if (logger.isInfoEnabled()) {
					logger.info("serverResponse: " + result);
				}
				
				// if the output file is not null, write the output in a synchronized way
				if (bw != null) {
					String[] data = LogFileUtils.extractEvaluationRelevantDataFromInputLine(messageBody);
					String requestId = data[0];
					String userId = data[1];
					String itemId = data[2];
					String domainId = data[3];

					synchronized (bw) {
						long timeStampAsLong = 0;
						try {
							timeStampAsLong = sdfLogDate.parse(this.messageTimestamp).getTime();
						} catch (Exception e) {
							logger.warn(e.toString(), e);
						}
						requestId = timeStampAsLong + "0" + ((resultLineCount++) %100); 
						try {
							bw.write("prediction\t" + requestId + "\t" + timeStampAsLong + "\t" + responseTime + "\t" + itemId+ "\t" + userId + "\t" + domainId + "\t" + result + "\t" + data[5]);
							bw.newLine();
						} catch (Exception e) {
							logger.warn(e.toString(), e);
						}
					}
				}
			}
		}
	}
}
