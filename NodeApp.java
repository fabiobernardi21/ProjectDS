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

public class NodeApp {
	static private Boolean recover = Boolean.FALSE, join = Boolean.FALSE;
	static private String remotePath = null; //path of the bootstamping node
	static private String ip = null; //ip of the bootstamping node
	static private String port = null; //port of the bootstramping node
	static private ActorRef client; //reference of the client that send a request
	static private int myId; // ID of the local node
	//Join packet
  public static class Join implements Serializable {
		int id;
		public Join(int id) {
			this.id = id;
		}
	}
	//DataResponseMessage -> the value answered by the server to the coordinator server
	public static class DataResponseMessage implements Serializable{
		String value;
		public DataResponseMessage(String value){
			this.value = value;
		}
		public String getValue(){
			return value;
		}
	}
	//DataMessage -> read or write message sent by the coordinator to the server
	public static class DataMessage implements Serializable {
		int key;
		String value;
		Boolean read;
		Boolean write;
		public DataMessage(int key, String value, Boolean read, Boolean write){
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
		public int getKey(){
			return key;
		}
		public String getValue(){
			return value;
		}
	}
	//AckMessage -> ack sent by the servet to the coodinator when write is succesfully done
	public static class AckMessage implements Serializable{
		Boolean ack;
		public AckMessage(Boolean ack){
			this.ack = ack;
		}
	}
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
		// The table of id-actorref that contain all the nodes
		private Map<Integer, ActorRef> nodes = new HashMap<>();
		//Table of key-value in the node
		private Map<Integer, String> data = new HashMap<>();

		//method called when a node is created
		public void preStart() {
			if (remotePath != null) {
    			getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
			}
			nodes.put(myId, getSelf());
		}
		//method used to make a write on a server in the system
		public void save_value(MessageRequest m){
			int serverid = find_server(m.getKey());
			System.out.println("Server "+serverid);
			nodes.get(serverid).tell(new DataMessage(m.getKey(),m.getValue(),Boolean.FALSE,Boolean.TRUE),getSelf());
		}
		//method used to find the right server in the system
		public int find_server(int key){
			List<Integer> list = new ArrayList<Integer>(nodes.keySet());
			Collections.sort(list);
			if(list.get(list.size()-1) < key){
				return list.get(0);
			}
			else {
				for(int i = 0; i < list.size(); i++){
					if(list.get(i) >= key){
						return list.get(i);
					}
				}
			}
			return 1;
		}
		//method that send a DataMessage to a specific server to read a value connected to a key
		public void read_value(int key){
			int serverid = find_server(key);
			nodes.get(serverid).tell(new DataMessage(key,null,Boolean.TRUE,Boolean.FALSE),getSelf());
		}
		//when the server receive a message control the message type and operate
    public void onReceive(Object message) {
			//if is a RequestNodelist the server send back the Nodelist
			if (message instanceof RequestNodelist) {
				getSender().tell(new Nodelist(nodes), getSelf());
			}
			//if is a Nodelist the server put the list of nodes in its local map and send a Join message
			else if (message instanceof Nodelist) {
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
				System.out.println("Messaggio client ricevuto");
				client = getSender();
				//if is a read call read_value function
				if(((MessageRequest)message).isRead()){
					read_value(((MessageRequest)message).getKey());
				}
				//if is leave call leave function
				if(((MessageRequest)message).isLeave()){
					System.out.println("Eseguo il leave");
				}
				//if is write call save_value function
				if(((MessageRequest)message).isWrite()){
					save_value(((MessageRequest)message));
				}
			}
			//if is a DataMessage from server coordinator
			else if (message instanceof DataMessage){
				System.out.println("Messaggio dato ricevuto");
				DataMessage m = ((DataMessage)message);
				//if is a read send back a DataResponseMessage with the value
				if (m.isRead()){
					System.out.println("Eseguo il read");
					String value = data.get(m.getKey());
					getSender().tell(new DataResponseMessage(value),getSelf());
				}
				//if is a write make the write and send back an AckMessage
				if(m.isWrite()){
					System.out.println("Eseguo il write");
					data.put(m.getKey(),m.getValue());
					getSender().tell(new AckMessage(Boolean.TRUE),getSelf());
				}
			}
			//if is an AckMessage send to client a Response message with ack
			else if(message instanceof AckMessage){
				Response r = new Response();
				r.fill(Boolean.TRUE,Boolean.FALSE,Boolean.FALSE,null);
				client.tell(r,getSelf());
			}
			//if is a DataResponseMessage send to client the value
			else if(message instanceof DataResponseMessage){
				Response r = new Response();
				r.fill(Boolean.FALSE,Boolean.TRUE,Boolean.FALSE,((DataResponseMessage)message).getValue());
				client.tell(r,getSelf());
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
