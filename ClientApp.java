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
		static private String remotePath = null; //path of the server contacted
		private static String ip = null, port = null,value = null;
		private static Boolean leave = Boolean.FALSE, read = Boolean.FALSE, write = Boolean.FALSE;
		private static int key = 0;

  public static class Client extends UntypedActor {
		//function called after the node is created
		public void preStart() {
			if(remotePath != null){
				//if we are in leave send a MessageRequest with leave
				if(leave){
					System.out.println("Send a leave message to "+ip+" "+port);
					MessageRequest m = new MessageRequest();
					m.fill("leave",key,value);
					getContext().actorSelection(remotePath).tell(m,getSelf());
				}
				//if we are in read send a MessageRequest with read key
				if(read){
					System.out.println("Send a read message ["+key+"] to "+ip+" "+port);
					MessageRequest m = new MessageRequest();
					m.fill("read",key,value);
					getContext().actorSelection(remotePath).tell(m,getSelf());
				}
				//if we are in write send a MessageRequest with write key value
				if(write){
					System.out.println("Send a write message ["+key+" "+value+"] to "+ip+" "+port);
					MessageRequest m = new MessageRequest();
					m.fill("write",key,value);
					getContext().actorSelection(remotePath).tell(m,getSelf());
				}
			}
		}
		//when the Client receive a message control the message type and operate
		public void onReceive(Object message) {

			//if is a Response it stamp the response received
			if (message instanceof Response) {
				Response r = ((Response)message);
				if(leave){
					leave = Boolean.FALSE;
					if(r.getLeave()){
						System.out.println("Leave done");
					}
					else System.out.println("Leave not done");
				}
				if(write){
					write = Boolean.FALSE;
					if(r.getWrite()){
						System.out.println("Write done");
					}
					else System.out.println("Write not done");
				}
				if(read){
					read = Boolean.FALSE;
					if(r.getRead()){
						r.stamp_value_version();
					}
					else System.out.println("Read not done");
				}
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
			else System.out.println("argument error");
		}
		else if (args.length == 4 ) { //read case
			ip = args[0];
		  port = args[1];
			if(args[2].equals("read")){
				read = Boolean.TRUE;
				key = Integer.valueOf(args[3]);
				remotePath = "akka.tcp://mysystem@"+ip+":"+port+"/user/node";
			}
			else System.out.println("argument error");
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
			else System.out.println("argument error");
		}
		else System.out.println("argument error");

		// Create the actor system
		final ActorSystem system = ActorSystem.create("mysystem", config);

		// Create a single node actor
		final ActorRef receiver = system.actorOf(
				Props.create(Client.class),	// actor class
				"client"										// actor name
				);
   }
}
