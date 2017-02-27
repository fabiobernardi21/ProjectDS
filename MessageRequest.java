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

public class MessageRequest implements Serializable {
	private int key;
	private String value;
	private String request;

	public void MessageRequest(){
		request = "null";
		value = "null";
		key = 0;
	}

	public void fill( String request, int key, String value){
		this.value = value;
		this.request = request;
		this.key = key;
	}

	public int getKey(){
		return key;
	}

	public String getValue(){
		return value;
	}

	public Boolean isRead(){
		if(request.equals("read")){
			return Boolean.TRUE;
		}
		else return Boolean.FALSE;
	}

	public Boolean isWrite(){
		if(request.equals("write")){
			return Boolean.TRUE;
		}
		else return Boolean.FALSE;
	}

	public Boolean isLeave(){
		if(request.equals("leave")){
			return Boolean.TRUE;
		}
		else return Boolean.FALSE;
	}

	public void stamp(){
		System.out.println(request+ " message to " +key+ " with value " +value);
	}
}
