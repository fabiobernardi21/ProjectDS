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


public class ClientApp {
		static private String remotePath = null; // Akka path of the bootstrapping peer

	  	public static class ClientRequest implements Serializable {}
	  	public static class RequestNodelist implements Serializable {}
	    public static class Nodelist implements Serializable {}

    public static class Client extends UntypedActor {

		
		public void preStart() {
			if(remotePath != null){
				MessageRequest m = new MessageRequest();
				m.fill("ciao","ciao",2);
				getContext().actorSelection(remotePath).tell(m,getSelf());
			}
		}

		public void onReceive(Object message) {
			unhandled(message);		// this actor does not handle any incoming messages
        }

    }
	
    public static void main(String[] args) {
		
		// Load the "application.conf"
		Config config = ConfigFactory.load("application");

		if (args.length == 2) {
			// Starting with a bootstrapping node
			String ip = args[0];
			String port = args[1];
    		// The Akka path to the bootstrapping peer
			remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node";
		}
		
		// Create the actor system
		final ActorSystem system = ActorSystem.create("mysystem", config);

		// Create a single node actor
		final ActorRef receiver = system.actorOf(
				Props.create(Client.class),	// actor class 
				"client"						// actor name
				);
    }
}
