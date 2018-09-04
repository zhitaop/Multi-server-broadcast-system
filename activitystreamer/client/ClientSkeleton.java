package activitystreamer.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class ClientSkeleton extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ClientSkeleton clientSolution;
	private static TextFrame textFrame;
	private int portnum;
	private String hostname;
	private Socket s = null;
	private DataInputStream in;
	private DataOutputStream out;
	private BufferedReader inreader;
	private PrintWriter outwriter;
	private JSONParser parser = new JSONParser();
	private boolean term=false;

	
	public static ClientSkeleton getInstance(){
		if(clientSolution==null){
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}
	
	public static void setTestFrame() {
		if(textFrame == null) {
			textFrame = new TextFrame();
		}
	}
	
	public ClientSkeleton(){
		
		portnum = Settings.getRemotePort();
		hostname = Settings.getRemoteHostname();
		try {
			s = new Socket(hostname,portnum);
		    in = new DataInputStream( s.getInputStream());
		    out =new DataOutputStream( s.getOutputStream());
		    inreader = new BufferedReader( new InputStreamReader(in));
		    outwriter = new PrintWriter(out, true);
		    log.info("connection established to "
		    		 + Settings.socketAddress(s));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		setTestFrame();
		start();
	}
	
	
	@SuppressWarnings("unchecked")	
	public void sendActivityObject(JSONObject activityObj){
		JSONObject obj = new JSONObject();

		obj.put("command", "ACTIVITY_MESSAGE");
		obj.put("username", Settings.getUsername());
		obj.put("secret", Settings.getSecret());
		obj.put("activity", activityObj);
		
		outwriter.println(obj.toString());
		outwriter.flush();
		//out.writeUTF(activityObj.toString());
	}
	
	
	@SuppressWarnings("unchecked")
	public void disconnect(){
		JSONObject obj = new JSONObject();

		obj.put("command", "LOGOUT");
		outwriter.println(obj.toString());
		outwriter.flush();
		term = true;
		
		try {
			inreader.close();
			outwriter.close();
			s.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public void login() {
		
		JSONObject obj = new JSONObject();

		if(Settings.getUsername().equals("anonymous")) {
			obj.put("command", "LOGIN");
			obj.put("username", Settings.getUsername());
		}
		
		else if(Settings.getSecret()!=null) {
			obj.put("command", "LOGIN");
			obj.put("username", Settings.getUsername());
			obj.put("secret", Settings.getSecret());
		} 
		else {
			Settings.setSecret(Settings.nextSecret());
			obj.put("command", "REGISTER");
			obj.put("username", Settings.getUsername());
			obj.put("secret", Settings.getSecret());
		}

		outwriter.println(obj.toString());
		outwriter.flush();
	}
	
	
	@SuppressWarnings("unchecked")
	public synchronized boolean process(String msg) {
		JSONObject obj = null;
		try {
			obj = (JSONObject) parser.parse(msg);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        String commandValue = (String) obj.get("command");
        
        if(commandValue.equals("ACTIVITY_BROADCAST")) {
        	JSONObject activity = (JSONObject) obj.get("activity");
			textFrame.setOutputText(activity);
			return false;
        }
        else if (commandValue.equals("REDIRECT")) {
            Long portValue =  ((Long)obj.get("port"));
            String hostname = (String) obj.get("hostname");
			try {
				inreader.close();
				outwriter.close();
				s.close();
				Settings.setRemoteHostname(hostname);
				Settings.setRemotePort(portValue.intValue());
				log.info("Connection redirected to "+ Settings.getRemoteHostname()+":"+
				Settings.getRemotePort());
				clientSolution = new ClientSkeleton();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return true;
        }
        else if (commandValue.equals("LOGIN_SUCCESS")) {
        	log.info(msg);
        	return false;
        }
        else if (commandValue.equals("REGISTER_SUCCESS")) {
        	log.info(msg);
			log.info("The secret for user "+Settings.getUsername()+" is: "+
        	Settings.getSecret());
        	obj = new JSONObject();
			obj.put("command", "LOGIN");
			obj.put("username", Settings.getUsername());
			obj.put("secret", Settings.getSecret());
			outwriter.println(obj.toString());
			outwriter.flush();
			return false;
        }

        else {
        	log.error(msg);
        	return true;
        }
	}
	
	
	public void run(){
		
		login();
		String data;
		try {
			while(!term &&(data = inreader.readLine())!=null) {
				
				term = process(data);
				
			}

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error("The connection has been closed unexpectedly");

		}
		try {
			inreader.close();
			outwriter.close();
			s.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.info("The connection to "+Settings.socketAddress(s)+" has been closed");

	}

}
