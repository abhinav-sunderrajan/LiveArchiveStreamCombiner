package mcomp.dissertation.helper;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

/**
 * A simple WebSocketServer implementation.
 */
public class StreamServer extends WebSocketServer {

	public StreamServer( int port ) throws UnknownHostException {
		super( new InetSocketAddress( port ) );
	}

	public StreamServer( InetSocketAddress address ) {
		super( address );
	}

	@Override
	public void onOpen( WebSocket conn, ClientHandshake handshake ) {
		this.sendToAll( "new connection: " + handshake.getResourceDescriptor() );
		System.out.println( conn.getRemoteSocketAddress().getAddress().getHostAddress() + "connected to the client" );
	}

	@Override
	public void onClose( WebSocket conn, int code, String reason, boolean remote ) {
		this.sendToAll( conn + " Connection closed" );
		System.out.println( conn + " closed connection with client" );
	}

	@Override
	public void onMessage( WebSocket conn, String message ) {
		this.sendToAll( message.getBytes() );
		System.out.println( conn + ": " + message );
	}


	@Override
	public void onError( WebSocket conn, Exception ex ) {
		ex.printStackTrace();
		if( conn != null ) {
			// some errors like port binding failed may not be assignable to a specific websocket
		}
	}
	
	
	/**
	 * Send byte array to client
	 * @param arr
	 */

	public void sendToAll( byte[] arr ) {
		Collection<WebSocket> con = connections();
		synchronized ( con ) {
			for( WebSocket c : con ) {
				c.send(arr);
			}
		}
	}
	
	/**
	 * Send String message to client
	 * @param message
	 */
	public void sendToAll( String message ) {
		Collection<WebSocket> con = connections();
		synchronized ( con ) {
			for( WebSocket c : con ) {
				c.send(message);
			}
		}
	}
}