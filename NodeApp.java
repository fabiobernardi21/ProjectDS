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
	static private int myId; // ID of the local node

  public static class Join implements Serializable {
		int id;
		public Join(int id) {
			this.id = id;
		}
	}

	public static class DataMessage implements Serializable {
		String value;
		Boolean read;
		Boolean write;
		public DataMessage(String value, Boolean read, Boolean write){
			this.value = value;
			this.read = read;
			this.write = write;
		}
	}
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
		private Map<Interger, String> data = new HashMap<>();

		public void preStart() {
			if (remotePath != null) {
    			getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
			}
			nodes.put(myId, getSelf());
		}

		//metodo per salvare il valore sul server
		public void save_value(MessageRequest m){}

		//metodo per trovare il server giusto
		public int find_server(int key){
			return 1;
		}

		//metodo per ricavare valore dal server
		public String return_value(int id){
			return null;
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
				System.out.println("Node " + id + " joined");
				nodes.put(id, getSender());
			}
            else if (message instanceof ClientRequest) {
				getSender().tell(new ClientRequest(), getSelf());
			}
			else if (message instanceof MessageRequest) {
				System.out.println("Messaggio ricevuto");
				if(((MessageRequest)message).isRead()){
					System.out.println("Eseguo il read");
				}
				if(((MessageRequest)message).isLeave()){
					System.out.println("Eseguo il leave");
				}
				if(((MessageRequest)message).isWrite()){
					System.out.println("Eseguo il write");
					save_value(((MessageRequest)message));
				}
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
