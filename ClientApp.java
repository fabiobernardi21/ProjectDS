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

public class ClientApp {
		static private String remotePath = null; // Akka path of the bootstrapping peer
		private static String ip = null, port = null,value = null;
		private static Boolean leave = Boolean.FALSE, read = Boolean.FALSE, write = Boolean.FALSE;
		private static int key = 0;

  public static class Client extends UntypedActor {

		public void preStart() {
			if(remotePath != null){
				if(leave){
					MessageRequest m = new MessageRequest();
					m.fill("leave",key,value);
					getContext().actorSelection(remotePath).tell(m,getSelf());
					leave = Boolean.FALSE;
				}
				if(read){
					MessageRequest m = new MessageRequest();
					m.fill("read",key,value);
					getContext().actorSelection(remotePath).tell(m,getSelf());
					read = Boolean.FALSE;
				}
				if(write){
					MessageRequest m = new MessageRequest();
					m.fill("write",key,value);
					getContext().actorSelection(remotePath).tell(m,getSelf());
					write = Boolean.FALSE;
				}
			}
		}

		public void onReceive(Object message) {
			if (message instanceof Response) {
				((Response)message).stamp_responce();
			}
			else unhandled(message);
    }
	}

  public static void main(String[] args) {
		// Load the "application.conf"
		Config config = ConfigFactory.load("application");

		if (args.length == 3 ) { //leave case
			ip = args[0];
			port = args[1];
			if(args[2].equals("leave")){
				leave = Boolean.TRUE;
				remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node";

			}
			else System.out.println("argument error1");
		}
		else if (args.length == 4 ) { //read case
			ip = args[0];
		  port = args[1];
			if(args[2].equals("read")){
				read = Boolean.TRUE;
				key = Integer.valueOf(args[3]);
				remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node";
			}
			else System.out.println("argument error2");
		}
		else if (args.length == 5 ) { //write case
			ip = args[0];
			port = args[1];
			if(args[2].equals("write")){
				write = Boolean.TRUE;
				key = Integer.valueOf(args[3]);
				value = args[4];
				remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node";
			}
			else System.out.println("argument error3");
		}
		else System.out.println("argument error4");

		// Create the actor system
		final ActorSystem system = ActorSystem.create("mysystem", config);

		// Create a single node actor
		final ActorRef receiver = system.actorOf(
				Props.create(Client.class),	// actor class
				"client"						// actor name
				);
   }
}
