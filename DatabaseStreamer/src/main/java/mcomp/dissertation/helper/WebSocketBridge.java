package mcomp.dissertation.helper;

import java.net.UnknownHostException;

import org.java_websocket.WebSocketImpl;

/**
 * 
 * The interface which connects the java application to browser.
 *
 */
public class WebSocketBridge {
	
	private StreamServer server;
	private static final int PUBLISH_PORT=8080;
	
	public WebSocketBridge(){
		try {
			server=new StreamServer(PUBLISH_PORT);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	
	public void startServer(){
		try {
			WebSocketImpl.DEBUG = true;
			server.start();
			System.out.println( "StreamServer started on port: " + server.getPort() );
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void sendMessage(byte[] message){
			server.sendToAll(message);
				
		}
	
	public void sendMessage(String message){
		server.sendToAll(message);
			
	}
	

}
