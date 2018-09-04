package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ArrayList<Connection> clientCon;
	private static ArrayList<Connection> serverCon;
	private static Connection parentCon = null;
	private static boolean term=false;
	private static Listener listener;
	protected static Control control = null;
	private JSONParser parser = new JSONParser();
	private static HashMap<String, String> userInfo;
	private static String id;
	private static ArrayList<String> serveridList;
	private static int knownServerCount;
	private static Connection registerCon;
    private static boolean registering=false;
    private static ArrayList<ServerInfo> serverInfo;

    private static ServerInfo backupServer = null;
    
    
	
	public static Control getInstance() {
		if(control==null){
			control=new Control();
		} 
		return control;
	}
	
	public Control() {
		// initialize the connections array
		clientCon = new ArrayList<Connection>();
		serverCon = new ArrayList<Connection>();
		userInfo = new HashMap<String, String>();
		id = Settings.nextSecret();
		serveridList = new ArrayList<String>();
		serverInfo = new ArrayList<ServerInfo>();
		// start a listener
		try {
			listener = new Listener();

		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: "+e1);
			System.exit(-1);
		}
		start();

	}
	
	@SuppressWarnings("unchecked")
	public void initiateConnection(){
		// make a connection to another server if remote hostname is supplied
		if(Settings.getRemoteHostname()!=null){
			try {
				Connection c = outgoingConnection(new Socket(Settings.getRemoteHostname(),Settings.getRemotePort()));
				parentCon = c;
				parentCon.setServerInfo(new ServerInfo(Settings.getRemoteHostname(),Settings.getRemotePort()));
				JSONObject obj = new JSONObject();
				obj.put("command", "AUTHENTICATE");
				obj.put("secret", Settings.getSecret());
				obj.put("port", Settings.getLocalPort());
				obj.put("hostname", Settings.getLocalHostname());
				obj.put("id", id);
				obj.put("load",clientCon.size());
				c.writeMsg(obj.toString());
				
				/*
				obj = new JSONObject();
				obj.put("command", "SERVER_LIST");
				obj.put("structure", serverStructure);
				c.writeMsg(obj.toString());
				*/
				log.info("connected to server at "+ Settings.getRemoteHostname());
			} catch (IOException e) {
				log.error("failed to make connection to "+Settings.getRemoteHostname()+":"+Settings.getRemotePort()+" :"+e);
				System.exit(-1);
			}
		}
	}
	
	
	/*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
	 */
	@SuppressWarnings("unchecked")
	public synchronized boolean process(Connection con,String msg){
		
		JSONObject obj = null;
		try {
			obj = (JSONObject) parser.parse(msg);
		} catch (ParseException e) {
			obj = new JSONObject();
			obj.put("command", "INVALID_MESSAGE");
			obj.put("info", "JSON parse error while parsing message");
			con.writeMsg(obj.toString());
			e.printStackTrace();
		}
        String commandValue = (String) obj.get("command");
        String secretValue = (String) obj.get("secret");
        String usernameValue = (String) obj.get("username");
        String hostnameValue = (String) obj.get("hostname");
        JSONObject activity = (JSONObject)obj.get("activity");
		JSONObject userinfo = (JSONObject)obj.get("userinfo");
        String idValue = (String) obj.get("id");
        
        Long portValue =  ((Long)obj.get("port"));
        Long loadValue =  ((Long)obj.get("load"));
		
        JSONObject jsonobj = new JSONObject();
        
		if (commandValue.equals("AUTHENTICATE")) {
			if(secretValue==null) {
				jsonobj.put("info" , "the received message did not contain a secret");
				return invalidMessage(con,jsonobj);			
			}
			else if(serverCon.contains(con)) {
				jsonobj.put("info" , "the server has already been authenticated");
				return invalidMessage(con,jsonobj);
			}
			else if (secretValue.equals(Settings.getSecret())) {
				
				jsonobj.put("command", "AUTHENTICATION_SUCCESS");
				jsonobj.put("id", id);
				con.writeMsg(jsonobj.toString());
				
				serverCon.add(con);
				clientCon.remove(con);	
				con.setServerInfo(new ServerInfo(idValue,hostnameValue,portValue.intValue(),loadValue.intValue()));			
				
				if(!userInfo.isEmpty()) {
					sendUserinfo(con);
				}
				if(parentCon != null) {
					sendBackupServer(con);
				}
				if(parentCon == null) {
					parentCon = con;
					parentCon.setServerInfo(new ServerInfo(idValue,hostnameValue, portValue.intValue(),loadValue.intValue()));
					sendBackupServer(con);
				}
			}
			else {
				//reply server authentication fail
				jsonobj.put("command", "AUTHENTICATION_FAIL");
				jsonobj.put("info", "the supplied secret is incorrect: "+secretValue);
				con.writeMsg(jsonobj.toString());
				return true;
			}
		}
		
		else if(commandValue.equals("AUTHENTICATION_SUCCESS")) {
			parentCon.getServerInfo().setId(idValue);
			updateBackupServer(false);
		}
		
		else if(commandValue.equals("BACKUP_SERVER")) {
		
			if(!idValue.equals(id)) {
				backupServer = new ServerInfo(hostnameValue,portValue.intValue());
				backupServer.setId(idValue);
			}else {
				backupServer = null;
			}
		}
		
		else if(commandValue.equals("USERINFO_UPDATE")) {
			
			for (Object key: userinfo.keySet()) {
				String username =(String) key;
				String secret = (String) userinfo.get(username);								
				userInfo.put(username, secret);
			}			
		}
		
		else if(commandValue.equals("AUTHENTICATION_FAIL")) {
			
			parentCon = null;
        	log.error(msg);
			return true;
		}
		
		
		else if(commandValue.equals("REGISTER")) {
			if(usernameValue==null||secretValue==null) {
				jsonobj.put("info" , "the register command did not contain a necessary field");
				return invalidMessage(con,jsonobj);
			}
			else if(con.getLoggedin()) {
				jsonobj.put("info" , "the client has already logged in on this connection");
				return invalidMessage(con,jsonobj);
			}
			else if(!checkUserExist(usernameValue)) {
				if(serverCon.size()==0) {
					userInfo.put(usernameValue, secretValue);
					jsonobj.put("command", "REGISTER_SUCCESS");
					jsonobj.put("info", "register success for "+usernameValue);
					con.writeMsg(jsonobj.toString());					
				
				} else {
					jsonobj.put("command", "LOCK_REQUEST");
					jsonobj.put("username", usernameValue);
					jsonobj.put("secret", secretValue);
					serverBroadcast(con,jsonobj.toString());
					knownServerCount = serveridList.size();
					registerCon = con;
					registering = true;
				}
			}
			else {
				jsonobj.put("command", "REGISTER_FAILED");
				jsonobj.put("info", usernameValue + " is already registered with the system");
				con.writeMsg(jsonobj.toString());
				return true;
			}
		}
		
		
		else if(commandValue.equals("LOCK_REQUEST")) {
			if(usernameValue==null||secretValue==null) {
				jsonobj.put("info" , "the register command did not contain a necessary field");
				return invalidMessage(con,jsonobj);
			}
			else if(!serverCon.contains(con)) {
				jsonobj.put("info" , "the server has not been authenticated yet");
				return invalidMessage(con,jsonobj);
			}
			else {
			serverBroadcast(con,msg);
			
			if(!checkUserExist(usernameValue)) {
				//serverbroadcast lock_allowed
				userInfo.put(usernameValue, secretValue);
				jsonobj.put("command", "LOCK_ALLOWED");
				jsonobj.put("username", usernameValue);
				jsonobj.put("secret", secretValue);
				serverAnnounce(jsonobj.toString());
			}
			else {
				//serverbroadcast lock_denied
				jsonobj.put("command", "LOCK_DENIED");
				jsonobj.put("username", usernameValue);
				jsonobj.put("secret", secretValue);
				serverAnnounce(jsonobj.toString());
			}
			}
		}
		
		else if(commandValue.equals("LOCK_ALLOWED")) {
			
			if(usernameValue==null||secretValue==null) {
				jsonobj.put("info" , "the register command did not contain a necessary field");
				return invalidMessage(con,jsonobj);
			}
			else if(!serverCon.contains(con)) {
				jsonobj.put("info" , "the server has not been authenticated yet");
				return invalidMessage(con,jsonobj);
			}
			else {
			serverBroadcast(con,msg);
			
			if(registering) {
				if(knownServerCount > 0) {
					knownServerCount--;
				}
				if(knownServerCount == 0) {
					userInfo.put(usernameValue, secretValue);
					jsonobj.put("command", "REGISTER_SUCCESS");
					jsonobj.put("info", "register success for "+usernameValue);
					registerCon.writeMsg(jsonobj.toString());
					registering = false;
				}
			}
			}
		}
		
		else if(commandValue.equalsIgnoreCase("LOCK_DENIED")) {
			
			if(usernameValue==null||secretValue==null) {
				jsonobj.put("info" , "the register command did not contain a necessary field");
				return invalidMessage(con,jsonobj);
			}
			else if(!serverCon.contains(con)) {
				jsonobj.put("info" , "the server has not been authenticated yet");
				return invalidMessage(con,jsonobj);
			}
			
			else {
			serverBroadcast(con,msg);
			
			if(userInfo.containsKey(usernameValue) && checkUserMatchSecret(usernameValue,secretValue)) {
				userInfo.remove(usernameValue);
			}
			
			if(registering) {
				jsonobj.put("command", "REGISTER_FAILED");
				jsonobj.put("info", usernameValue + " is already registered with the system");
				registerCon.writeMsg(jsonobj.toString());
				registering = false;
				return true;
			}
			}
		}
		
		else if(commandValue.equals("SERVER_ANNOUNCE")) {
			if(idValue==null||hostnameValue==null||portValue==null||loadValue==null) {
				jsonobj.put("info" , "the server announce did not contain a necessary field");
				return invalidMessage(con,jsonobj);
			}
			else if(!serverCon.contains(con)) {
				jsonobj.put("info" , "the server has not been authenticated yet");
				return invalidMessage(con,jsonobj);
			}
			else {
			if(!serveridList.contains(idValue)) {
				serveridList.add(idValue);
				serverInfo.add(new ServerInfo(idValue, hostnameValue, portValue.intValue(), loadValue.intValue()));
			} 
			else {
				for (ServerInfo s:serverInfo) {
					if (s.getId().equals(idValue))
					s.setLoad(loadValue.intValue());
				}
			}
			
			log.debug("received announcement from server ID "+ idValue+" load "+loadValue+" at "
					+hostnameValue+":"+portValue);
			
			serverBroadcast(con,msg);
			}
		}
		
		else if (commandValue.equals("LOGIN")) {
			if(usernameValue==null) {
				jsonobj.put("info" , "the login command did not contain a username");
				return invalidMessage(con,jsonobj);			
			}
			else if(usernameValue.equals("anonymous")) {
				con.setLoggedUsername("anonymous");
				con.setLoggedin(true);
				jsonobj.put("command", "LOGIN_SUCCESS");
				jsonobj.put("info", "logged in as user "+usernameValue);
				con.writeMsg(jsonobj.toString());
				
				return redirect(con);
			}
			else if(userInfo.containsKey(usernameValue)) {
				if(secretValue==null) {
					jsonobj.put("info" , "the login command did not contain a secret");
					return invalidMessage(con,jsonobj);				
				}
				else if (secretValue.equals(userInfo.get(usernameValue))) {
					con.setLoggedUsername(usernameValue);
					con.setLoggedSecret(secretValue);
					con.setLoggedin(true);
					jsonobj.put("command", "LOGIN_SUCCESS");
					jsonobj.put("info", "logged in as user "+usernameValue);
					con.writeMsg(jsonobj.toString());
					
					return redirect(con);
				}
				else {
					con.setLoggedin(false);
					jsonobj.put("command", "LOGIN_FAILED");
					jsonobj.put("info", "attempt to login with wrong secret");
					con.writeMsg(jsonobj.toString());
					return true;
				}
			}
			else {
				con.setLoggedin(false);
				jsonobj.put("command", "LOGIN_FAILED");
				jsonobj.put("info", "user "+usernameValue +" does not exist on this server");
				con.writeMsg(jsonobj.toString());
				return true;
			}
		}
		else if(commandValue.equals("LOGOUT")) {
			return true;
		}
		
		else if(commandValue.equals("ACTIVITY_MESSAGE")) {
			if(usernameValue==null) {
				jsonobj.put("info" , "the activity message did not contain a username");
				return invalidMessage(con,jsonobj);
			}
			else if(activity==null) {
				jsonobj.put("info" , "the activity message did not contain an activity object");
				return invalidMessage(con,jsonobj);
			}
			else if(!usernameValue.equals("anonymous")&&secretValue==null) {
				jsonobj.put("info" , "the activity message did not contain a secret");
				return invalidMessage(con,jsonobj);
			}
			else if(!con.getLoggedin()) {
				jsonobj.put("command","AUTHTENTICATION_FAIL");
				jsonobj.put("info", "user has not logged in");
				con.writeMsg(jsonobj.toString());
				return true;
			}
			else if((usernameValue.equals("anonymous")&& con.getLoggedUsername().equals("anonymous")) ||
					(con.getLoggedUsername().equals(usernameValue) && con.getLoggedSecret().equals(secretValue))) {
				activity.put("authenticated_user", con.getLoggedUsername());
				jsonobj.put("command", "ACTIVITY_BROADCAST");
				jsonobj.put("activity",activity);
				allBroadcast(con,jsonobj.toString());
				con.writeMsg(jsonobj.toString());
			}
			else {
				jsonobj.put("command","AUTHTENTICATION_FAIL");
				jsonobj.put("info", "the supplied secret is incorrect");
				con.writeMsg(jsonobj.toString());
				return true;
			}
		}
		else if(commandValue.equals("ACTIVITY_BROADCAST")) {
			if(activity==null) {
				jsonobj.put("info" , "the meesage did not contain a activity object");
				return invalidMessage(con,jsonobj);
			}
			else if(!serverCon.contains(con)) {
				jsonobj.put("info" , "the server has not been authenticated yet");
				return invalidMessage(con,jsonobj);
			}
			else{
				allBroadcast(con,msg);
			}
		}
		
		else if(commandValue.equals("INVALID_MESSAGE")) {
        	log.error(msg);
			return true;
		}
		
		else if(commandValue.equals("DELETE_SERVER")) {
			
			serveridList.remove(idValue);
			for(int i=0;i<serverInfo.size();i++) {
				if(serverInfo.get(i).getId().equals(idValue)){
					serverInfo.remove(i);
				}
			}
			serverBroadcast(con,msg);
		}
			
		else  {
			jsonobj.put("info" , "the received message did not contain a valid command");
			return invalidMessage(con,jsonobj);
		}
		return false;
	
	}
	
	public synchronized void allBroadcast(Connection con, String msg) {
		for (Connection c:clientCon) {
			if (!c.equals(con)) {
				c.writeMsg(msg);
			}
		}
		for (Connection c:serverCon) {
			if (!c.equals(con)) {
				c.writeMsg(msg);
			}
		}
	}
	
	public synchronized void serverBroadcast(Connection con, String msg) {
		for (Connection c:serverCon) {
			if (!c.equals(con)) {
				c.writeMsg(msg);
			}
		}
	}
	
	public synchronized void serverAnnounce (String msg) {
		for (Connection c:serverCon) {
			c.writeMsg(msg);
		}
	}
	
	@SuppressWarnings("unchecked")
	public synchronized boolean redirect(Connection con) {

		JSONObject obj = new JSONObject();
		for(ServerInfo s:serverInfo){
			if(s.getLoad()<(clientCon.size()-1)) {
				obj.put("command", "REDIRECT");
				obj.put("hostname", s.getHostname());
				obj.put("port",s.getPort());
				con.writeMsg(obj.toString());
				return true;
			}						
		}
		return false;
	}
	
	@SuppressWarnings("unchecked")
	public synchronized boolean invalidMessage(Connection con,JSONObject obj) {
		obj.put("command", "INVALID_MESSAGE");
		con.writeMsg(obj.toString());
		return true;
	}
	
	/*
	 * The connection has been closed by the other party.
	 */
	@SuppressWarnings("unchecked")
	public synchronized void connectionClosed(Connection con){
		if(serverCon.contains(con)) {
			JSONObject obj = new JSONObject();
			serveridList.remove(con.getServerInfo().getId());
			for(int i=0;i<serverInfo.size();i++) {
				if(serverInfo.get(i).getId().equals(con.getServerInfo().getId())){
					serverInfo.remove(i);

				}
			}		
			obj.put("command", "DELETE_SERVER");
			obj.put("id",con.getServerInfo().getId());
			serverAnnounce(obj.toString());
		}
		if(!term) {
			clientCon.remove(con);
			serverCon.remove(con);
		}

		
		if(con==parentCon) {
			if(backupServer!=null) {
				try {
					Connection c = outgoingConnection(new Socket(backupServer.getHostname(),backupServer.getPort()));
					parentCon = c;
					parentCon.setServerInfo(new ServerInfo(backupServer.getHostname(),backupServer.getPort()));
					parentCon.getServerInfo().setId(backupServer.getId());
					JSONObject obj = new JSONObject();
					obj.put("command", "AUTHENTICATE");
					obj.put("secret", Settings.getSecret());
					obj.put("port", Settings.getLocalPort());
					obj.put("hostname", Settings.getLocalHostname());
					obj.put("id", id);
					obj.put("load",clientCon.size());
					c.writeMsg(obj.toString());

					
					
					log.info("connected to server at "+ backupServer.getHostname()+":"+backupServer.getPort());
				} catch (IOException e) {
					log.error("failed to make connection to "+backupServer.getHostname()+":"+backupServer.getPort()+" :"+e);
					System.exit(-1);
				}
			} else {
				if(!serverCon.isEmpty()) {// serverinfo is not empty, yet the
					parentCon = serverCon.get(0);
					updateBackupServer(true);
				}else {
					parentCon = null;
				}
			}

		}
		
	}
	
	/*
	 * A new incoming connection has been established, and a reference is returned to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException{
		log.debug("incoming connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		clientCon.add(c);
		return c;
		
	}
	
	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException{
		log.debug("outgoing connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		serverCon.add(c);
		return c;
	}
	
	@Override
	public void run(){
		if(Settings.getRemoteHostname()==null){
			log.info("secret of this server is "+Settings.getSecret());
		}
		log.info("using activity interval of "+Settings.getActivityInterval()+" milliseconds");
		while(!term){
			// do something with 5 second intervals in between
			try {
				Thread.sleep(Settings.getActivityInterval());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			}
			if(!term){
				log.debug("connected client number: "+clientCon.size());
				log.debug("connected server number: "+serverCon.size());
				log.debug("known server number: "+serveridList.size());	
				
				if(parentCon != null) {
					log.debug("parent host: "+parentCon.getServerInfo().getHostname());
					log.debug("parent port: "+parentCon.getServerInfo().getPort());
				}
				if(backupServer != null) {
					log.debug("backup host: "+backupServer.getHostname());
					log.debug("backup port: "+backupServer.getPort());
				}
				log.debug("----------------------------------------");
						
				term=doActivity();
			}
			
		}
		log.info("closing "+clientCon.size()+" client connections");
		// clean up
		for(Connection connection : clientCon){
			connection.closeCon();
		}
		log.info("closing "+serverCon.size()+" server connections");
		// clean up
		for(Connection connection : serverCon){
			connection.closeCon();
		}
		listener.setTerm(true);
	}
	
	@SuppressWarnings("unchecked")
	public boolean doActivity(){
		
		JSONObject obj = new JSONObject();
		obj.put("command", "SERVER_ANNOUNCE");
		obj.put("id", id );
		obj.put("load",clientCon.size());
		obj.put("hostname", Settings.getLocalHostname() );
		obj.put("port", Settings.getLocalPort());
		serverAnnounce(obj.toString());

		return false;
	}
	
	@SuppressWarnings("unchecked")
	public void sendUserinfo(Connection con) {
		JSONObject obj = new JSONObject();
		JSONObject userinfo = new JSONObject();
		obj.put("command", "USERINFO_UPDATE");
		for(HashMap.Entry<String, String> entry:userInfo.entrySet()) {
			userinfo.put(entry.getKey(), entry.getValue());
		}
		obj.put("userinfo", userinfo);
		
		con.writeMsg(obj.toString());
	}
	
	@SuppressWarnings("unchecked")
	public void sendBackupServer(Connection con) {
		
		JSONObject obj = new JSONObject();
		if(parentCon!=null) {
			obj.put("command", "BACKUP_SERVER");
			obj.put("hostname", parentCon.getServerInfo().getHostname());
			obj.put("port", parentCon.getServerInfo().getPort());
			obj.put("id", parentCon.getServerInfo().getId());
			con.writeMsg(obj.toString());
		}
	}
	
	@SuppressWarnings("unchecked")
	public void updateBackupServer(boolean includeParent) {
		JSONObject obj = new JSONObject();
		if(parentCon!=null) {
			obj.put("command", "BACKUP_SERVER");
			obj.put("hostname", parentCon.getServerInfo().getHostname());
			obj.put("port", parentCon.getServerInfo().getPort());
			obj.put("id", parentCon.getServerInfo().getId());
			
			if(includeParent) {
				serverAnnounce(obj.toString());
			} else {			
				for (Connection c:serverCon) {
					if(!c.equals(parentCon)) {
						c.writeMsg(obj.toString());
					}
				}
			}
		}
	}
	
	public final void setTerm(boolean t){
		term=t;
	}
	
	public final ArrayList<Connection> getConnections() {
		return clientCon;
	}
	
	public boolean checkUserExist(String username) {
		return userInfo.containsKey(username);
	}
	public boolean checkUserMatchSecret(String username, String secret) {
		return secret.equals(userInfo.get(username));
	}
		
	
}
