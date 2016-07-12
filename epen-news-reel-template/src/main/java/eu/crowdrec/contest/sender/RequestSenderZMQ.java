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

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * This class simulates an online evaluation by re-playing an previously recorded stream.
 * 
 * @author andreas

 */

public class RequestSenderZMQ {
		

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
	public static void sender(String serverURL, final String inLogFileName, final String outLogFile) {
		
		// initialize the 0MQ
		final Context context = ZMQ.context(1);
		final Socket clientSocket = context.socket(ZMQ.REQ);
		if (serverURL == null) {
			serverURL = "tcp://127.0.0.1:8088";
		}
		clientSocket.connect(serverURL);
		
		// handle the log file
		// check type of file
		boolean is_gzip = false;
		if (inLogFileName.toLowerCase().endsWith(".gz")) {
			is_gzip = true;
		}
		
		// try to read the defined logFile
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			// if outLogFile name is not null, create an output file
			if (outLogFile != null && outLogFile.length() > 0) {
				bw = new BufferedWriter(new FileWriter(new File(outLogFile), false));
			}
			
			InputStream is = new FileInputStream(inLogFileName);
			// support gZip files
			if (is_gzip) {
				is = new GZIPInputStream(is);
			}
			
			// read the log file line by line
			br = new BufferedReader(new InputStreamReader(is));
			try {
				for (String line = br.readLine(); line != null; line = br.readLine()) {
					
					// ignore invalid lines and header
					if (line.startsWith("null") || line.startsWith("#")) {
						continue;
					}
					
					// send parameters to http server and get response.
					clientSocket.send(line);
					String result = clientSocket.recvStr();
					
					boolean answerExpected = false;
					if (line.contains("\"event_type\": \"recommendation_request\"")) {
						answerExpected = true;
					}
					
					if (answerExpected) {
						System.out.println("serverResponse: " + result);
						
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
									bw.write("prediction\t" + requestId + "\t" + timeStamp + "\t" + itemId+ "\t" + userId + "\t" + domainId + "\t" + result);
									bw.newLine();
								}catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			System.err.println("logFile not found e:" + e.toString());
		} catch (IOException e) {
			System.err.println("reading the logFile failed e:" + e.toString());
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					System.err.println("close read-log file failed");
				}
			}
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
					System.err.println("close write-log file failed");
				}
			}
		}
	}

	/**
	 * Start the logFile sender.
	 * 
	 * @param args
	 *            String[]{hostname:port, inLogfileName [, outLogFileName]}
	 */
	public static void main(String[] args) {
		if (args.length == 2 || args.length == 3) {
			sender(args[0], args[1], args[2]);
		} else {
			System.err.println("wrong number of parameters.");
			System.err.println("usage: java RequestSender <hostName>:<port> <inLogfileName> [<outLogFile]");
			System.err.println(".gz files are supported.");
		}
		System.out.println("Program finished normally, call System.exit()");
		System.exit(0);
	}
}
