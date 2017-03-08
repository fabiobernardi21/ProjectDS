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
import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.Random;



public class NodeApp {
	static private Boolean recover = Boolean.FALSE, join = Boolean.FALSE;
	static private String remotePath = null; //path of the bootstamping node
	static private String ip = null; //ip of the bootstamping node
	static private String port = null; //port of the bootstramping node
	static private ActorRef client; //reference of the client that send a request
	static private int myId; // ID of the local node
	static private int n = 3, w = 2, r = 2;

	public static class ImRecovered implements Serializable{
		private int id;
		public ImRecovered(int id){
			this.id = id;
		}
		public int getId(){
			return id;
		}
	}
	public static class RequestDataRecovery implements Serializable{
		private int senderid;
		public RequestDataRecovery(int senderid){
			this.senderid = senderid;
		}
		public int getSenderid(){
			return senderid;
		}

	}
	public static class DataRecovery implements Serializable{
		private Data d;
		private int key;
		public DataRecovery(Data d, int key){
			this.d = d;
			this.key = key;
		}
		public Data getData(){
			return d;
		}
		public int getKey(){
			return key;
		}
	}
	//Data that contain value and version stored on the servers map with a key
	public static class Data implements Serializable {
		private String value;
		private int version;
		public Data(){
			value = null;
			version = 0;
		}
		public Data(String value, int version){
			this.value = value;
			this.version = version;
		}
		public String getValue(){
			return value;
		}
		public int getVersion(){
			return version;
		}
	}
	//Join packet
  public static class Join implements Serializable {
		private int id;
		public Join(int id) {
			this.id = id;
		}
		public int getId(){
			return id;
		}
	}
	//Leave packet
	public static class Leave implements Serializable {
		private int id;
		public Leave(int id){
			this.id = id;
		}
		public int getId(){
			return id;
		}
	}
	//NodeDataBase -> packet that send all the database
	public static class NodeDataBase implements Serializable{
		Map<Integer, Data> database;
		public NodeDataBase(Map<Integer,Data> database){
			this.database = database;
		}
	}
	//NodeDataBase -> packet that send all the database
	public static class NodeData implements Serializable{
		private Data d;
		private int key;
		public NodeData(Data d, int key){
			this.d = d;
			this.key = key;
		}
		public Data getData(){
			return d;
		}
		public int getKey(){
			return key;
		}
	}
	//DataResponseMessage -> the value answered by the server after a read to the coordinator
	public static class DataResponseMessage implements Serializable{
		private Data data;
		public DataResponseMessage(Data data){
			this.data = data;
		}
		public Data getData(){
			return data;
		}
	}
	//DataMessage -> read or write message sent by the coordinator to the server
	public static class DataMessage implements Serializable {
		private int key;
		private String value;
		private Boolean read;
		private Boolean write;
		private int version;
		public DataMessage(int key, String value ,int version, Boolean read, Boolean write){
			this.key = key;
			this.value = value;
			this.read = read;
			this.write = write;
			this.version = version;
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
		public String getValue(){
			return value;
		}
		public int getKey(){
			return key;
		}
		public int getVersion(){
			return version;
		}
	}
	//Ack -> ack sent by the server to the coodinator when the server is ready for write
	public static class Ack implements Serializable{
		int version;
		public Ack(int version){
			this.version = version;
		}
		public int getVersion(){
			return version;
		}
	}
	//AckRequest -> ack sent by the coodrinator to n servers
	public static class AckRequest implements Serializable{
		int key;
		public AckRequest(int key){
			this.key = key;
		}
		public int getKey(){
			return key;
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
	public static class RequestNodelistRecovery implements Serializable{}
	public static class NodelistRecovery implements Serializable{
		Map<Integer, ActorRef> nodes;
		public NodelistRecovery(Map<Integer, ActorRef> nodes) {
			this.nodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(nodes));
		}
	}
  public static class Node extends UntypedActor {
		//Table of id-actorref that contain all the nodes
		private Map<Integer, ActorRef> nodes = new HashMap<>();
		//Table of key-value in the node
		private Map<Integer, Data> data = new HashMap<>();
		//list of nodes that answer to read
		private ArrayList<DataResponseMessage> read_answer = new ArrayList<>();
		//list of nodes that answer to write
		private ArrayList<Ack> write_answer = new ArrayList<>();
		//write message sent by client and buffered on coordinator to wait ack from servers
		private MessageRequest write_message = new MessageRequest();
		//list of server ids where AckRequest is sent by coordinator
		private ArrayList<Integer> serverid = new ArrayList<Integer>();
		//random variable to generate time delays
		private Random rand = new Random();
		//time to wait answer by nodes
		private int timeout = 1000;

		public int max_wr(){
			if(r > w) return r;
			else if(r < w) return w;
			else return w;
		}
  	//method that return true if there are enough write answers
		public Boolean enough_read(){
			return read_answer.size() >= max_wr();
		}
		//method that return true if there are enough write answers
		public Boolean enough_write(){
			return write_answer.size() >= max_wr();
		}
		//method used to make a write on a server in the system sending an ackrequest packet
		public void save_value(MessageRequest m){
			write_message = m;
			//finds the servers that are responsable from the new data
			serverid = find_server(m.getKey());
			//sends an AckRequest message to the nodes that are responsable for that data
			int version;
			for(int j = 0; j < serverid.size(); j++){
				if(serverid.get(j) == myId){
					if(!data.containsKey(m.getKey())){
						version = 0;
					}
					else version = data.get(m.getKey()).getVersion();
					nodes.get(serverid.get(j)).tell(new Ack(version),getSelf());
				}
				else nodes.get(serverid.get(j)).tell(new AckRequest(m.getKey()),getSelf());
			}
			//schedules the Runnable operation after a timeout
			getContext().system().scheduler().scheduleOnce(Duration.create(timeout, TimeUnit.MILLISECONDS),
			new Runnable() {
				public void run() {
					if (enough_write()) {
						//if w is enough sends a DataMessage to the responsable nodes and sends the Response back to the client
						int version = 0;
						for(int j = 0; j < write_answer.size(); j++){
							if(write_answer.get(j).getVersion() > version){
								version = write_answer.get(j).getVersion();
							}
						}
						for(int j = 0; j < serverid.size(); j++){
							nodes.get(serverid.get(j)).tell(new DataMessage(write_message.getKey(),write_message.getValue(),version+1,Boolean.FALSE,Boolean.TRUE),getSelf());
						}
						serverid.clear();
						Response response = new Response();
						response.fill(Boolean.TRUE,Boolean.FALSE,Boolean.FALSE,null,0);
						client.tell(response,getSelf());
						write_answer.clear();
					}
					else{
						Response response = new Response();
						response.fill(Boolean.FALSE,Boolean.FALSE,Boolean.FALSE,null,0);
						client.tell(response,getSelf());
					}
				}
			}, getContext().system().dispatcher());
		}
		//method used to find the right server in the system
		public ArrayList<Integer> find_server(int key){
			ArrayList<Integer> ids = new ArrayList<Integer>();
			List<Integer> list = new ArrayList<Integer>(nodes.keySet());
			int start = 0, q = 0;
			Collections.sort(list);
			if(list.get(list.size()-1) < key){
				start = 0;
			}
			else {
				for(int j = 0; j < list.size(); j++){
					if(list.get(j) >= key){
						start = j;
						break;
					}
				}
			}
			while(q < n){
				ids.add(list.get(start));
				start=(start+1)%list.size();
				q++;
			}
			return ids;
		}
		//method to find the right version of value
		public int find_version(int key){
			List<Integer> list = new ArrayList<Integer>(data.keySet());
			if (list.size() != 0) {
				for(int i = 0; i < list.size(); i++){
					if(list.get(i) == key){
						Data d = data.get(key);
						return d.getVersion()+1;
					}
				}
			}
			return 0;
		}
		//method that send a DataMessage to a specific server to read a value connected to a key
		public void read_value(int key){
			//finds the servers that have the data and sends to them a DataMessage of type read and if it is itself responsable sends itself a DataResponseMessage
		 	serverid = find_server(key);
			for(int j = 0; j < serverid.size(); j++){
				if(serverid.get(j) == myId){
					Data d = data.get(key);
					nodes.get(serverid.get(j)).tell(new DataResponseMessage(d),getSelf());
				}
				else nodes.get(serverid.get(j)).tell(new DataMessage(key,null,0,Boolean.TRUE,Boolean.FALSE),getSelf());
			}
			getContext().system().scheduler().scheduleOnce(Duration.create(timeout, TimeUnit.MILLISECONDS),
			new Runnable() {
				public void run() {
					if (enough_read()) {
						//if it has enough DataResponseMessage controls the maximum between the version and send back the linked data in the Response to the client
						int max_version = 0;
						int index = 0;
						for (int i = 0; i < read_answer.size(); i++ ) {
							if (max_version < read_answer.get(i).getData().getVersion()) {
								max_version = read_answer.get(i).getData().getVersion();
								index = i;
							}
						}
						Response response = new Response();
						String value = read_answer.get(index).getData().getValue();
						int version = max_version;
						response.fill(Boolean.FALSE,Boolean.TRUE,Boolean.FALSE,value,version);
						client.tell(response,getSelf());
						read_answer.clear();
					}else{
						Response response = new Response();
						response.fill(Boolean.FALSE,Boolean.FALSE,Boolean.FALSE,null,0);
						client.tell(response,getSelf());
					}
				}
			}, getContext().system().dispatcher());
		}
		//method that write on the storage of the node
		public void write_file(){
			try {
				String path = ("./Storage.txt");
				File file = new File(path);
			  FileWriter fileWriter = new FileWriter(file);
				List<Integer> list = new ArrayList<Integer>(data.keySet());
				fileWriter.write("");
        for (int i=0;i<list.size();i++){
					fileWriter.write(list.get(i) + " " + data.get(list.get(i)).getValue() + " " + data.get(list.get(i)).getVersion()+"\n");
				}
			  fileWriter.flush();
			  fileWriter.close();
			}catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void delete_file(){
			try{
	      File file = new File("Storage.txt");
	      if(file.delete()){
	      }else{
					System.out.println("Delete operation is failed.");
	      }
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		public void upload_file(){
			try {
			 FileReader f;
			 f=new FileReader("./Storage.txt");

			 BufferedReader b;
			 b=new BufferedReader(f);

			 String s = null;

			 while(true) {
				 s=b.readLine();
				 if(s != null){
					 String[] parts = s.split(" ",3);
					 Data d = new Data(parts[1],Integer.parseInt(parts[2]));
					 data.put(Integer.parseInt(parts[0]),d);
				 }
				 else break;
			 }
		 }catch (IOException e) {
			 System.out.println("Error file not found");
		 }
		}
		//method called when a node is created
		public void preStart() {
			if (remotePath != null) {
				if (join == Boolean.TRUE) {
					nodes.clear();
					data.clear();
					delete_file();//clean the old Storage.txt
					write_file();
					getContext().actorSelection(remotePath).tell(new RequestNodelist(), getSelf());
					join = Boolean.FALSE;
				}
				else if (recover == Boolean.TRUE){
					upload_file();
					getContext().actorSelection(remotePath).tell(new RequestNodelistRecovery(), getSelf());
					recover = Boolean.FALSE;
				}
			}
			else {
				//bootstramping node case
				nodes.clear();
				data.clear();
				delete_file();
				write_file();
			}
			nodes.put(myId, getSelf());
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
			//if it is Join the servers perform the joining
			else if (message instanceof Join) {
				int id = ((Join)message).getId();
				System.out.println("Node " +id+ " joined");
				nodes.put(id, getSender());
				List<Integer> list_key_nodes = new ArrayList<Integer>(nodes.keySet());
				List<Integer> list_key_data = new ArrayList<Integer>(data.keySet());
				Collections.sort(list_key_data);
				Collections.sort(list_key_nodes);
				//find the next node to the one that is joining
				int after = 0;
				if(list_key_nodes.get(list_key_nodes.size()-1) < id){
					after = 0;
				}
				else {
					for(int j = 0; j < list_key_nodes.size(); j++){
						if(list_key_nodes.get(j) > id){
							after = j;
							break;
						}
					}
				}
				//the next server sends to the joining node its list of data
				if (list_key_nodes.get(after) == myId) {
					Map <Integer,Data> database = new HashMap<>();
					for(int j = 0; j < list_key_data.size(); j++){
						Data d = data.get(list_key_data.get(j));
						database.put(list_key_data.get(j),d);
					}
					getSender().tell(new NodeDataBase(database),getSelf());
				}
				//all the nodes remove the data that became unusefull after the new node joined
				Boolean finded = Boolean.FALSE;
				for(int j = 0; j<list_key_data.size(); j++){
					serverid = find_server(list_key_data.get(j));
					for (int i = 0;i<serverid.size();i++) {
						if(serverid.get(i) == myId){
							finded = Boolean.TRUE;
							break;
						}
						finded = Boolean.FALSE;
					}
					if (finded == Boolean.FALSE) {
						data.remove(list_key_data.get(j));
						write_file();
					}
				}
			}
			//if is a MessageRequest from client
			else if (message instanceof MessageRequest) {
				client = getSender();
				//if is a read call read_value function
				if(((MessageRequest)message).isRead()){
					System.out.println("Client read request received");
					read_value(((MessageRequest)message).getKey());
				}
				//if is leave call leave function
				if(((MessageRequest)message).isLeave()){
					System.out.println("Client leave request received");
					nodes.remove(myId);//remove its id from its node list and sends to others nodes a message di tipo Leave
					for (ActorRef n: nodes.values()) {
						n.tell(new Leave(myId), getSelf());
					}
					//sends the data (NodeData) to the nodes that became responsable after its leaving
					List<Integer> list_key_data = new ArrayList<Integer>(data.keySet());
					for(int j = 0; j<list_key_data.size(); j++){
						serverid = find_server(list_key_data.get(j));
						for (int i = 0;i<serverid.size();i++) {
							nodes.get(serverid.get(i)).tell(new NodeData(data.get(list_key_data.get(j)),list_key_data.get(j)),getSelf());
						}
					}
					data.clear();
					nodes.clear();
					delete_file();
					//after the cleaning the memory sends the response to the client
					Response response = new Response();
					response.fill(Boolean.FALSE,Boolean.FALSE,Boolean.TRUE,null,0);
					client.tell(response,getSelf());
					try{
						TimeUnit.MILLISECONDS.sleep(2000);
					} catch (InterruptedException ie) {
						System.out.println("NODE:Timer error");
					}
					System.exit(0);
				}
				//if is write call save_value function
				if(((MessageRequest)message).isWrite()){
					System.out.println("Client write request received");
					save_value(((MessageRequest)message));
				}
			}
			//if is a DataMessage from server coordinator
			else if (message instanceof DataMessage){
				DataMessage m = ((DataMessage)message);
				//if is a read send back a DataResponseMessage with the value
				if (m.isRead()){
					//if it is read sends DataResponseMessage if it has the data
					if(data.containsKey(m.getKey())){
						Data d = data.get(m.getKey());
						getSender().tell(new DataResponseMessage(d),getSelf());
					}
				}
				//if is a write make the write
				if(m.isWrite()){
					//if it is a write find the latest version and write the DataMessage
					Data d = new Data(m.getValue(),m.getVersion());
					System.out.println("Write done");
					data.put(m.getKey(),d);
					write_file();
				}
			}
			//if is a AckRequest sent by coordinator, send back an Ack
			else if (message instanceof AckRequest){
				AckRequest ar = ((AckRequest)message);
				int version;
				if(!data.containsKey(ar.getKey())){
					version = 0;
				}
				else version = data.get(ar.getKey()).getVersion();
				getSender().tell(new Ack(version),getSelf());
			}
			//if is an Ack from server, wait W Ack messages and after send them the write and send back response to client
			else if(message instanceof Ack){
				write_answer.add(((Ack)message));
			}
			//if is a DataResponseMessage from server wait R DataResponseMessage and after send to client the last version
			else if(message instanceof DataResponseMessage){
				//the coordinator collects the DataResponseMessage
				read_answer.add(((DataResponseMessage)message));
			}
			else if (message instanceof NodeDataBase) {
				NodeDataBase nd = ((NodeDataBase)message);
				data.putAll(nd.database);
				List<Integer> list_key_data = new ArrayList<Integer>(data.keySet());
				Collections.sort(list_key_data);
				Boolean finded = Boolean.FALSE;
				for(int j = 0; j<list_key_data.size(); j++){
					serverid = find_server(list_key_data.get(j));
					for (int i = 0;i<serverid.size();i++) {
						if(serverid.get(i) == myId){
							finded = Boolean.TRUE;
							break;
						}
						finded = Boolean.FALSE;
					}
					if (finded == Boolean.FALSE) {
						data.remove(list_key_data.get(j));
					}
				}
				write_file();
			}
			else if (message instanceof NodeData) {
				NodeData nd = ((NodeData)message);
				//if the nodes receves a data (NodeData) that it hasn't, it inserts it in its database
				if (data.containsKey(nd.getKey()) == Boolean.FALSE){
					data.put(nd.getKey(),nd.getData());
					write_file();
				}
			}
			else if (message instanceof Leave){
				//the nodes that receives a message of type Leave remove the sender from their node list
				nodes.remove(((Leave)message).getId());
			}
			else if (message instanceof RequestNodelistRecovery){
				//bootstramping node send back the NodelistRecovery to the recovering node
				getSender().tell(new NodelistRecovery(nodes),getSelf());
			}
			else if (message instanceof NodelistRecovery){
				//the recovering node copy the NodelistRecovery that the bootstramping node send to it
				Boolean finded = Boolean.FALSE;
				nodes.putAll(((NodelistRecovery)message).nodes);
				List<Integer> id_node_list = new ArrayList<Integer>(nodes.keySet());
				Collections.sort(id_node_list);
				//the recovering node send to other nodes a message ImRecovered to update his ActorRef in the map of the others
				for(int j = 0; j < id_node_list.size(); j++){
					nodes.get(id_node_list.get(j)).tell(new ImRecovered(myId), getSelf());
				}
				List<Integer> list_key_data = new ArrayList<Integer>(data.keySet());
				//check if its data are corrected or it has to delete some unusefull data
				for(int j = 0; j<list_key_data.size(); j++){
					serverid = find_server(list_key_data.get(j));
					for (int i = 0;i<serverid.size();i++) {
						if(serverid.get(i) == myId){
							finded = Boolean.TRUE;
							break;
						}
						finded = Boolean.FALSE;
					}
					if (finded == Boolean.FALSE) {
						data.remove(list_key_data.get(j));
						write_file();
					}
				}
				/*
				//this part is to  update on its memory all the new data that are written while it is crashed
				for (int j = 0; j < id_node_list.size(); j++){
					nodes.get(id_node_list.get(j)).tell(new RequestDataRecovery(myId),getSelf());
				}
				*/
			}
			/*
			else if (message instanceof RequestDataRecovery){
				RequestDataRecovery rdr = ((RequestDataRecovery)message);
				List<Integer> list_key_data = new ArrayList<Integer>(data.keySet());
				List<Integer> id_node_list = new ArrayList<Integer>(nodes.keySet());
				for(int j = 0; j<list_key_data.size(); j++){
					serverid = find_server(list_key_data.get(j));
					for (int i = 0;i<serverid.size();i++) {
						if (serverid.get(i) == rdr.getSenderid()){
							//System.out.println("Send back the data");
							getSender().tell(new DataRecovery(data.get(list_key_data.get(j)),list_key_data.get(j)),getSelf());
						}
					}
				}
			}
			else if (message instanceof DataRecovery){
				DataRecovery dr = ((DataRecovery)message);
				if (data.containsKey(dr.getKey()) == false){
					data.put(dr.getKey(),dr.getData());
					write_file();
				}
				else{
					if(dr.getData().getVersion() > data.get(dr.getKey()).getVersion()){
						data.put(dr.getKey(),dr.getData());
						write_file();
					}
				}
			}
			*/
			else if (message instanceof ImRecovered){
				//used to update the ActorRef
				ImRecovered ir = ((ImRecovered)message);
				nodes.put(ir.getId(),getSender());
			}
			else unhandled(message);		// this actor does not handle any incoming messages
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
