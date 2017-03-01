import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import java.lang.Boolean;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.Random;

public class NodeApp {
	static private Boolean recover = Boolean.FALSE, join = Boolean.FALSE;
	static private String remotePath = null; //path of the bootstamping node
	static private String ip = null; //ip of the bootstamping node
	static private String port = null; //port of the bootstramping node
	static private ActorRef client; //reference of the client that send a request
	static private int myId; // ID of the local node
	static private int n = 1, w = 1, r = 1;

	//Data that contain value and version stored on the servers map with a key
	public static class Data implements Serializable {
		String value;
		int version;
		public Data(){
			value = null;
			version = 0;
		}
		public Data(String value, int version){
			this.value = value;
			this.version = version;
		}
		public String getValue(){
			return value;
		}
		public int getVersion(){
			return version;
		}
	}

	//Join packet
  public static class Join implements Serializable {
		int id;
		public Join(int id) {
			this.id = id;
		}
	}
	//DataResponseMessage -> the value answered by the server after a read to the coordinator
	public static class DataResponseMessage implements Serializable{
		Data data;
		public DataResponseMessage(Data data){
			this.data = data;
		}
		public Data getData(){
			return data;
		}
	}
	//DataMessage -> read or write message sent by the coordinator to the server
	public static class DataMessage implements Serializable {
		int key;
		String value;
		Boolean read;
		Boolean write;
		public DataMessage(int key, String value , Boolean read, Boolean write){
			this.key = key;
			this.value = value;
			this.read = read;
			this.write = write;
		}
		public Boolean isRead(){
			if(read){
				return Boolean.TRUE;
			}
			else return Boolean.FALSE;
		}
		public Boolean isWrite(){
			if(write){
				return Boolean.TRUE;
			}
			else return Boolean.FALSE;
		}
		public String getValue(){
			return value;
		}
		public int getKey(){
			return key;
		}
	}
	//Ack -> ack sent by the server to the coodinator when the server is ready for write
	public static class Ack implements Serializable{}
	//AckRequest -> ack sent by the coodrinator to n servers
	public static class AckRequest implements Serializable{}
	//RequestNodelist -> packet to request the nodelist to the bootstramping node
  public static class RequestNodelist implements Serializable {}
	//Nodelist -> packet to sent the nodelist to the server joined
  public static class Nodelist implements Serializable {
		Map<Integer, ActorRef> nodes;
		public Nodelist(Map<Integer, ActorRef> nodes) {
			this.nodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(nodes));
		}
	}

  public static class Node extends UntypedActor {
		//Table of id-actorref that contain all the nodes
		private Map<Integer, ActorRef> nodes = new HashMap<>();
		//Table of key-value in the node
		private Map<Integer, Data> data = new HashMap<>();
		//list of nodes that answer to read
		private ArrayList<DataResponseMessage> read_answer = new ArrayList<>();
		//list of nodes that answer to write
		private ArrayList<Ack> write_answer = new ArrayList<>();
		//write message sent by client and buffered on coordinator to wait ack from servers
		private MessageRequest write_message = new MessageRequest();
		//list of server ids where AckRequest is sent by coordinator
		private ArrayList<Integer> serverid = new ArrayList<Integer>();
		//random variable to generate time delays
		private Random rand = new Random();

  	//method that return true if there are enough write answers
		public Boolean enough_read(){
			return read_answer.size() >= r;
		}
		//method that return true if there are enough write answers
		public Boolean enough_write(){
			return write_answer.size() >= w;
		}
		//method used to make a write on a server in the system sending an ackrequest packet
		public void save_value(MessageRequest m){
			write_message = m;
			serverid = find_server(m.getKey());
			System.out.println("COORDINATOR:Send ACK requests to N nodes");
			for(int j = 0; j < serverid.size(); j++){
				if(serverid.get(j) == myId){
					nodes.get(serverid.get(j)).tell(new Ack(),getSelf());
				}
				else nodes.get(serverid.get(j)).tell(new AckRequest(),getSelf());
			}
		}
		//method used to find the right server in the system
		public ArrayList<Integer> find_server(int key){
			ArrayList<Integer> ids = new ArrayList<Integer>();
			List<Integer> list = new ArrayList<Integer>(nodes.keySet());
			int start = 0, q = 0;
			Collections.sort(list);
			if(list.get(list.size()-1) < key){
				start = 0;
			}
			else {
				for(int j = 0; j < list.size(); j++){
					if(list.get(j) >= key){
						start = j;
						break;
					}
				}
			}
			while(q <= n){
				ids.add(list.get(start));
				start=(start+1)%list.size();
				q++;
			}
			System.out.println("Find servers "+ids+" where data is replicated");
			return ids;
		}
		//method to find the right version of value
		public int find_version(int key){
			List<Integer> list = new ArrayList<Integer>(data.keySet());
			System.out.println("Find the last version available");
			for(int i = 0; i < list.size(); i++){
				if(list.get(i) == key){
					Data d = data.get(key);
					return d.getVersion()+1;
				}
			}
			return 0;
		}
		//method that send a DataMessage to a specific server to read a value connected to a key
		public void read_value(int key){
		 	serverid = find_server(key);
			System.out.println("COORDINATOR:Send a data read request to N nodes");
			for(int j = 0; j < serverid.size(); j++){
				if(serverid.get(j) == myId){
					Data d = data.get(key);
					nodes.get(serverid.get(j)).tell(new DataResponseMessage(d),getSelf());
				}
				else nodes.get(serverid.get(j)).tell(new DataMessage(key,null,Boolean.TRUE,Boolean.FALSE),getSelf());
			}
		}
		//method that write on the storage of the node
		public void write_file(){
			try {
				String path = ("./Storage.txt");
				File file = new File(path);
			  FileWriter fileWriter = new FileWriter(file);
				List<Integer> list = new ArrayList<Integer>(nodes.keySet());
				Integer k;
				Data d = new Data();
				String val;
				int ver;
        for (int i=0;i<list.size();i++){
					k=list.get(i);
				  d=data.get(k);
					val=d.getValue();
					ver=d.getVersion();
					fileWriter.write("" + k + " " + val + " " + ver + "\n");
				}
			  fileWriter.flush();
			  fileWriter.close();
			}catch (IOException e) {
				e.printStackTrace();
			}
		}

		//method called when a node is created
		public void preStart() {
			if (remotePath != null) {
				System.out.println("Send a nodelist request to bootstramping node");
				getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
			}
			nodes.put(myId, getSelf());
		}
		//when the server receive a message control the message type and operate
    public void onReceive(Object message) {
			//if is a RequestNodelist the server send back the Nodelist
			if (message instanceof RequestNodelist) {
				System.out.println("RequestNodelist packet rx -> send back Nodelist");
				getSender().tell(new Nodelist(nodes), getSelf());
			}
			//if is a Nodelist the server put the list of nodes in its local map and send a Join message
			else if (message instanceof Nodelist) {
				System.out.println("Nodelist packet rx -> send to all nodes a Join");
				nodes.putAll(((Nodelist)message).nodes);
				for (ActorRef n: nodes.values()) {
					n.tell(new Join(myId), getSelf());
				}
			}
			//if is a Join the server put sender on map because it is joined
			else if (message instanceof Join) {
				int id = ((Join)message).id;
				System.out.println("Node " +id+ " joined");
				nodes.put(id, getSender());
			}
			//if is a MessageRequest from client
			else if (message instanceof MessageRequest) {
				client = getSender();
				//if is a read call read_value function
				if(((MessageRequest)message).isRead()){
					System.out.println("COORDINATOR:Client read request received");
					read_value(((MessageRequest)message).getKey());
				}
				//if is leave call leave function
				if(((MessageRequest)message).isLeave()){
					System.out.println("COORDINATOR:Client leave request received");
					System.exit(0);
				}
				//if is write call save_value function
				if(((MessageRequest)message).isWrite()){
					System.out.println("COORDINATOR:Client write request received");
					save_value(((MessageRequest)message));
				}
			}
			//if is a DataMessage from server coordinator
			else if (message instanceof DataMessage){
				DataMessage m = ((DataMessage)message);
				//if is a read send back a DataResponseMessage with the value
				if (m.isRead()){
					System.out.println("NODE:Coordinator read request received");
					Data d = data.get(m.getKey());
					System.out.println("NODE:DataResponseMessage sent back to coordinator");
					try {
						int randomNum = rand.nextInt((3500 - 2500) + 1) + 2000;
						System.out.println("NODE:Delay "+randomNum+" ms");
		  			TimeUnit.MILLISECONDS.sleep(randomNum);
					} catch (InterruptedException ie) {
						System.out.println("NODE:Timer error");
					}
					getSender().tell(new DataResponseMessage(d),getSelf());
				}
				//if is a write make the write
				if(m.isWrite()){
					System.out.println("NODE:Coordinator write request received");
					int version = find_version(m.getKey());
					Data d = new Data(m.getValue(),version);
          write_file();
					System.out.println("NODE:Write done");
					data.put(m.getKey(),d);
				}
			}
			//if is a AckRequest sent by coordinator, send back an Ack
			else if (message instanceof AckRequest){
				System.out.println("NODE:Write AckRequest received from coordinator");
				System.out.println("NODE:Ack sent back to coordinator");
				try {
					int randomNum = rand.nextInt((3500 - 2500) + 1) + 2000;
					System.out.println("NODE:Delay "+randomNum+" ms");
					TimeUnit.MILLISECONDS.sleep(randomNum);
				} catch (InterruptedException ie) {
					System.out.println("NODE:Timer error");
				}
				getSender().tell(new Ack(),getSelf());
			}
			//if is an Ack from server, wait W Ack messages and after send them the write and send back response to client
			else if(message instanceof Ack){
				System.out.println("COORDINATOR:Ack received from node");
				write_answer.add(((Ack)message));
				getContext().system().scheduler().scheduleOnce(Duration.create(3000, TimeUnit.MILLISECONDS),
  			new Runnable() {
    			public void run() {
						if (enough_write()) {
							System.out.println("COORDINATOR:I have enough ack to write on N nodes");
							System.out.println("COORDINATOR:Send to nodes a write DataMessage");
							for(int j = 0; j < serverid.size(); j++){
								nodes.get(serverid.get(j)).tell(new DataMessage(write_message.getKey(),write_message.getValue(),Boolean.FALSE,Boolean.TRUE),getSelf());
							}
							serverid.clear();
							System.out.println("COORDINATOR:Send back to client the response");
							Response response = new Response();
							response.fill(Boolean.TRUE,Boolean.FALSE,Boolean.FALSE,null,0);
							client.tell(response,getSelf());
							write_answer.clear();
						}
					}
				}, getContext().system().dispatcher());
			}
			//if is a DataResponseMessage from server wait R DataResponseMessage and after send to client the last version
			else if(message instanceof DataResponseMessage){
				read_answer.add(((DataResponseMessage)message));
				getContext().system().scheduler().scheduleOnce(Duration.create(3000, TimeUnit.MILLISECONDS),
  			new Runnable() {
    			public void run() {
						if (enough_read()) {
							System.out.println("COORDINATOR:I have enough respose to read on N nodes");
							int max_version = 0;
							int index = 0;
							System.out.println("COORDINATOR:Choose the data with latest version");
							for (int i = 0; i < read_answer.size(); i++ ) {
								if (max_version < read_answer.get(i).getData().getVersion()) {
									max_version = read_answer.get(i).getData().getVersion();
									index = i;
								}
							}
							System.out.println("COORDINATOR:Send back to client the response");
							Response response = new Response();
							String value = read_answer.get(index).getData().getValue();
							int version = max_version;
							response.fill(Boolean.FALSE,Boolean.TRUE,Boolean.FALSE,value,version);
							client.tell(response,getSelf());
							read_answer.clear();
						}
    			}
				}, getContext().system().dispatcher());
			}
			else
        	unhandled(message);		// this actor does not handle any incoming messages
      }
    }

  public static void main(String[] args) {

		// Load the "application.conf"
		Config config = ConfigFactory.load("application");
		myId = config.getInt("nodeapp.id");

		if (args.length == 0){
			System.out.println("Starting disconnected node " + myId);
		}
		else if (args.length == 3){
			ip = args[1];
			port = args[2];
			if(args[0].equals("join")){
				join = Boolean.TRUE;
				remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node";
				System.out.println("Starting node " + myId + "; bootstrapping node: " + ip + ":"+ port);
			}
			else if(args[0].equals("recover")){
				recover = Boolean.TRUE;
				remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node";
			}
			else{
			  System.out.println("Argument error for node application");
			}
		}

		if (args.length != 0 && args.length != 3) {
			System.out.println("Wrong number of arguments: [action, ip, port]");
			return;
		}

		// Create the actor system
		final ActorSystem system = ActorSystem.create("mysystem", config);

		// Create a single node actor
		final ActorRef receiver = system.actorOf(
				Props.create(Node.class),	// actor class
				"node"					        	// actor name
				);
		return;
    }
}
