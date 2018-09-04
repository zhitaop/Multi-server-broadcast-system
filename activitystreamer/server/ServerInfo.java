package activitystreamer.server;

public class ServerInfo {

	private String id;
	private String hostname;
	private int port;
	private int load;
	
	public ServerInfo(String id, String hostname, int port, int load) {
		this.id = id;
		this.hostname = hostname;
		this.port = port;
		this.load = load;
	}
	
	public ServerInfo(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}
	
	public String getId() {
		return id;
	}
	
	public String getHostname() {
		return hostname;
	}
	
	public int getPort() {
		return port;
	}
	
	public int getLoad() {
		return load;
	}
	
	public void setLoad(int load) {
		this.load = load;
	}
	public void setId(String id) {
		this.id = id;
	}

}
