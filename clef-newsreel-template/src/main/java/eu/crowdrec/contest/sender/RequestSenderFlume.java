/*
Copyright (c) 2015, TU Berlin
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;


/**
 * This class acts as a flume source. A log file is appended line by line to a flume stream.
 * 
 * @author andreas
 */

public class RequestSenderFlume {
		
    /**
     * The flume hostName
     */
    public static String hostname;
    
    /**
     * The flume port
     */    
    public static String port;
    
    /**
     * The flume client
     */     
    private static RequestSenderFlume client;

	/**
	 * read logFile then sends it as a flume stream.
	 * 
	 * @param inLogFileName
	 *            path to log file. That can be a zip file or text file.
	 * @param outLogFile
	 *            path to outLog file. The outLog file should be analyzed by the evaluator.
	 *            if the filename is null; no output will be generated
	 * @param hostName
	 * @param port
	 */
	public static void sender(final String inLogFileName, final String hostname, final String port) {
	
		 // Setup the RPC connection
		RequestSenderFlume.hostname = hostname;
		RequestSenderFlume.port = port;
		RpcClient client = RpcClientFactory.getDefaultInstance(hostname, Integer.parseInt(port));
		
		// handle the log file
		// check type of file
		boolean is_gzip = false;
		if (inLogFileName.toLowerCase().endsWith(".gz")) {
			is_gzip = true;
		}
		
		// try to read the defined logFile
		BufferedReader br = null;
		try {		
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
					
					// create a flume event and send it to the client, encode in utf-8
					Event event = EventBuilder.withBody(line, Charset.forName("UTF-8"));
					try {
						client.append(event);
					} catch (EventDeliveryException e) {
						e.printStackTrace();
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
					System.err.println("close log file failed");
				}
			}
			try {
				client.close();
			} catch (Exception e) {
				System.err.println("close flume client failed");
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
		if (args.length == 3) {
			sender(args[2], args[0], args[1]);
		} else {
			System.err.println("wrong number of parameters.");
			System.err.println("usage: java RequestSenderFlume <hostName>:<port> <inLogfileName>");
			System.err.println(".gz files are supported.");
		}
		System.out.println("Program finished normally, call System.exit()");
		System.exit(0);
	}
}
