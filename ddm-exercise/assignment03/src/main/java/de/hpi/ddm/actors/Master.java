package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
	}

	public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		this.welcomeData = welcomeData;
		this.userEntries = new HashMap<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data
	public static class UserEntry implements Serializable{
		private static final long serialVersionUID = 3304081602659723997L;
		private int userId;
		private String userName;
		private String passwordCharacters;
		private int passwordLength;
		private String encryptedPassword;
		private ArrayList<String> encryptedHints = new ArrayList<>();

		private String decryptedPassword;
		private ArrayList<String> decryptedHints;
		private int unsolvedHints;

		public UserEntry(String[] entryLine){
			this.userId = Integer.parseInt(entryLine[0]);
			this.userName = entryLine[1];
			this.passwordCharacters = entryLine[2];
			this.passwordLength = Integer.parseInt(entryLine[3]);
			this.encryptedPassword = entryLine[4];
			this.encryptedHints.addAll(Arrays.asList(entryLine).subList(5, entryLine.length-1));

			this.decryptedPassword = null;
			this.decryptedHints = new ArrayList<>();
			this.unsolvedHints = this.encryptedHints.size();
		}
	}


	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ActorRef largeMessageProxy;
	private final BloomFilter welcomeData;

	String allCharacters = null;

	private long startTime;

	private final HashMap<Integer, UserEntry> userEntries;
	private final LinkedList<Worker.TaskMessage> taskQueue = new LinkedList<>();

	private boolean isInitialRead = true;
	private boolean hasReceivedEverything = false;
	private int usersToCrack = 0;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(Worker.FinishedTaskMessage.class, this::handle)
				.match(Worker.HintMessage.class, this::handle)
				.match(Worker.GotSomeCrackBro.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void handle(BatchMessage message) throws InterruptedException {
		this.log().info("Receiving BatchMessage " + message);
		// TODO: This is where the task begins:
		// - The Master received the first batch of input records.
		// - To receive the next batch, we need to send another ReadMessage to the reader.
		// - If the received BatchMessage is empty, we have seen all data for this task.
		// - We need a clever protocol that forms sub-tasks from the seen records, distributes the tasks to the known workers and manages the results.
		//   -> Additional messages, maybe additional actors, code that solves the subtasks, ...
		//   -> The code in this handle function needs to be re-written.
		// - Once the entire processing is done, this.terminate() needs to be called.

		// Info: Why is the input file read in batches?
		// a) Latency hiding: The Reader is implemented such that it reads the next batch of data from disk while at the same time the requester of the current batch processes this batch.
		// b) Memory reduction: If the batches are processed sequentially, the memory consumption can be kept constant; if the entire input is read into main memory, the memory consumption scales at least linearly with the input size.
		// - It is your choice, how and if you want to make use of the batched inputs. Simply aggregate all batches in the Master and start the processing afterwards, if you wish.

		/** 1. Master gets the recods of all password entries form the reader sequentially
		 *	3
		 * 	in cracking actual passwords:
		 *		3.1 give workers all the hint-hashes oof
		 *			do I already know that hash?
		 *		-> if not:
		 *	compute hashes
		 *
		 *		3.2 give workers task: crack password for user (which is just the)
		 *
		 *
		**/

		if (message.getLines().isEmpty()) {
			this.hasReceivedEverything = true;
			return;
		}

		// TODO: split up batches more intelligently
		int lines = message.getLines().size();
		int taskBatchSize = Math.min(lines, 10);
		// create user entries from read lines
		ArrayList<Worker.UserHint> hints = new ArrayList<>();
		for (String[] line : message.getLines()){
			UserEntry userEntry = new UserEntry(line);
			this.usersToCrack++;
			if (allCharacters == null) {
				allCharacters = userEntry.passwordCharacters;
			}
			this.userEntries.put(userEntry.getUserId(), userEntry);
			Worker.UserHint hint = new Worker.UserHint(userEntry.getUserId(), userEntry.getEncryptedHints());
			hints.add(hint);
			// create one task for every possible missing character each (with hints)
			if(hints.size() >= taskBatchSize) {

				for (int i = 0; i < allCharacters.length(); i += 1) {
					String chars = allCharacters.substring(0, i) + allCharacters.substring(i + 1);
					Worker.TaskMessage taskMessage = new Worker.HintTaskMessage(chars, allCharacters.charAt(i), hints);
					taskQueue.add(taskMessage);
				}
				hints =  new ArrayList<>();
			}
		}
		// generate remaining tasks if necessary
		if(hints.size() > 0){
			for (int i = 0; i < allCharacters.length(); i += 1) {
				String chars = allCharacters.substring(0, i) + allCharacters.substring(i + 1);
				Worker.TaskMessage taskMessage = new Worker.HintTaskMessage(chars, allCharacters.charAt(i), hints);
				taskQueue.add(taskMessage);
			}
		}

		if(isInitialRead){
			// assign initial tasks to workers
			for(ActorRef worker: workers){
				Worker.TaskMessage nextTask = taskQueue.pollFirst();
				if (nextTask != null) {
					this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(nextTask, worker), this.self());
				}
			}
			isInitialRead = false;
		}

		// Fetch further lines from the Reader
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void terminate() {
		this.log().info("Starting the REAPEEERRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR");

		this.collector.tell(new Collector.PrintMessage(), this.self());

		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());

		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}

		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.log().info("Registered {}", this.sender());;;
		Worker.TaskMessage nextTask = taskQueue.pollFirst();
		if (nextTask != null) {
			this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(nextTask, this.sender()), this.self());
		}
	}

	protected void handle(Worker.FinishedTaskMessage message){
		this.log().info(sender().toString() + " finished their Task.");
		Worker.TaskMessage nextTask = taskQueue.pollFirst();
		if (nextTask != null) {
			this.log().info("Assigning new task to: " + this.sender().toString());
			this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(nextTask, this.sender()), this.self());
		}
	}
	protected void handle(Worker.GotSomeCrackBro message){
		this.log().info(sender().toString() + " crack? CRACK ?! CRACKED SOMETHING!!!!!!!!!");
																													//perfect
		this.collector.tell(new Collector.CollectMessage(message.getUserId() + ";" + this.userEntries.get(message.getUserId()).getUserName() + ";" + message.getDecryptedPassword()), this.self());

		usersToCrack--;
		if(this.hasReceivedEverything && usersToCrack < 1){
			this.terminate();
		}
	}

	protected void handle(Worker.HintMessage message){
		this.log().info("Got Hint from " + sender().toString());
		UserEntry user = userEntries.get(message.getUserId());
		String chars = user.getPasswordCharacters().replaceFirst(""+message.getMissingChar(), ""); // well :/
		user.setPasswordCharacters(chars);
		user.setUnsolvedHints(user.getUnsolvedHints() - 1);
		if(user.getUnsolvedHints() <= 0){
			// create task on crack
			taskQueue.add(new Worker.CrackTaskMessage(user.getUserId(), user.passwordCharacters, user.passwordLength, user.getEncryptedPassword()));
		}
	}

	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}
}
