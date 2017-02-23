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


public class MessageRequest implements Serializable{

	private String value;
	private String request;
	private int key;
    
	public void MessageRequest(){
		request = "null";
		value = "null";
		key = 0;
	}

	public void fill(String value, String request, int key){
		this.value = value;
		this.request = request;
		this.key = key;
	}

	public void stamp(){
		System.out.println("Message " +key+ " value " +value+ " request " +request);
	}
	
	
}