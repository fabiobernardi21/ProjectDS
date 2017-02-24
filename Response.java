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

public class Response implements Serializable {
  private Boolean write;
  private Boolean leave;
  private Boolean read;
  private String value;
  private int version;

  public void Responce(){
    write = Boolean.FALSE;
    leave = Boolean.FALSE;
    read = Boolean.FALSE;
    value = null;
    version = 0;
  }

  public void fill(Boolean write,Boolean read, Boolean leave, String value, int version){
    this.write = write;
    this.read = read;
    this.leave = leave;
    this.value = value;
    this.version = version;
  }

  public void stamp_responce(){
    if (write) {
      System.out.println("Write eseguito");
    }
    if (read) {
      System.out.println("Value = " +value+" Version = "+version);
    }
    if (leave) {
      System.out.println("Leave eseguito");
    }
  }
}
