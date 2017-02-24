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
	static private String remotePath = null, ip = null, port = null; // Akka path of the bootstrapping peer
	static private ActorRef client;
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

  public static class RequestNodelist implements Serializable {}
  public static class Nodelist implements Serializable {
		Map<Integer, ActorRef> nodes;
		public Nodelist(Map<Integer, ActorRef> nodes) {
			this.nodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(nodes));
		}
	}

  public static class Node extends UntypedActor {

		// The table of all nodes in the system id->ref
		private Map<Integer, ActorRef> nodes = new HashMap<>();
		//Table of value in the node
		private Map<Integer, String> data = new HashMap<>();

		public void preStart() {
			if (remotePath != null) {
    			getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
			}
			nodes.put(myId, getSelf());
		}

		//metodo per salvare il valore sul server
		public void save_value(MessageRequest m){
			int serverid = find_server(m.getKey());
			System.out.println("Server "+serverid);
			nodes.get(serverid).tell(new DataMessage(m.getKey(),m.getValue(),Boolean.FALSE,Boolean.TRUE),getSelf());
		}

		//metodo per trovare il server giusto
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

    public void onReceive(Object message) {
			if (message instanceof RequestNodelist) {
				getSender().tell(new Nodelist(nodes), getSelf());
			}
			else if (message instanceof Nodelist) {
				nodes.putAll(((Nodelist)message).nodes);
				for (ActorRef n: nodes.values()) {
					n.tell(new Join(myId), getSelf());
				}
			}
			else if (message instanceof Join) {
				int id = ((Join)message).id;
				System.out.println("Node " +id+ " joined");
				nodes.put(id, getSender());
			}
			else if (message instanceof MessageRequest) {
				System.out.println("Messaggio client ricevuto");
				client = getSender();
				if(((MessageRequest)message).isRead()){
					read_value(((MessageRequest)message).getKey());
				}
				if(((MessageRequest)message).isLeave()){
					System.out.println("Eseguo il leave");
				}
				if(((MessageRequest)message).isWrite()){
					save_value(((MessageRequest)message));
				}
			}
			else if (message instanceof DataMessage){
				System.out.println("Messaggio dato ricevuto");
				DataMessage m = ((DataMessage)message);
				if (m.isRead()){
					System.out.println("Eseguo il read");
					String value = data.get(m.getKey());
					getSender().tell(new DataResponseMessage(value),getSelf());
				}
				if(m.isWrite()){
					System.out.println("Eseguo il write");
					data.put(m.getKey(),m.getValue());
					getSender().tell(new AckMessage(Boolean.TRUE),getSelf());
				}
			}
			else if(message instanceof AckMessage){
				Response r = new Response();
				r.fill(Boolean.TRUE,Boolean.FALSE,Boolean.FALSE,null);
				client.tell(r,getSelf());
			}
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
			  System.out.println("argument error for node application");
			}
			ip = args[1];
			port = args[2];
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
				"node"						// actor name
				);
		return;
    }
}
