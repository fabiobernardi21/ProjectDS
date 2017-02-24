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
<<<<<<< HEAD
//io sono fabio
=======
//prova filippo
>>>>>>> origin/master

public class NodeApp {
	static private String remotePath = null; // Akka path of the bootstrapping peer
	static private int myId; // ID of the local node

    public static class Join implements Serializable {
		int id;
		public Join(int id) {
			this.id = id;
		}
	}

	public static class ClientRequest implements Serializable {}
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

		public void preStart() {
			if (remotePath != null) {
    			//getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
			}
			nodes.put(myId, getSelf());
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
				}
			}
			else
        	unhandled(message);		// this actor does not handle any incoming messages
        }
    }

    public static void main(String[] args) {

		if (args.length != 0 && args.length !=2 ) {
			System.out.println("Wrong number of arguments: [remote_ip remote_port]");
			return;
		}

		// Load the "application.conf"
		Config config = ConfigFactory.load("application");
		myId = config.getInt("nodeapp.id");
		if (args.length == 2) {
			// Starting with a bootstrapping node
			String ip = args[0];
			String port = args[1];
    		// The Akka path to the bootstrapping peer
			remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node";
			System.out.println("Starting node " + myId + "; bootstrapping node: " + ip + ":"+ port);
		}
		else
			System.out.println("Starting disconnected node " + myId);

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
