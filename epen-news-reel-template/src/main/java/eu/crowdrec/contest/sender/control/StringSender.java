package eu.crowdrec.contest.sender.control;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * This class enables sending strings given on the command line using ZMQ.
 *  
 * @author andreas
 *
 */
public class StringSender {
	
	/**
	 * Send a string using ZMQ. The program has been originally used for sending control messages in Idommar.
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if ( args.length < 2 ) {
			System.out.println("missing parameters");
			return;
		}

		try {
			// read the command line arguments and create a ZMQ context.
			String address = args[0];
			String command = args[1];
			Context context = ZMQ.context(1);
			Socket controlSocket = context.socket(ZMQ.REQ);
			//System.out.println("ALGO: ZMQ created context");
			
			// configure the ZMQ connection
			controlSocket.setReceiveTimeOut(10000);
			controlSocket.connect(address);
			
			// send the command
			controlSocket.send(command);
			//System.out.println("ZMQ connected to socket");
			
			// close the socket and terminate the context.
			controlSocket.close();
			context.term();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
