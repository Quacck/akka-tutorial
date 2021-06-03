package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import akka.cluster.Member;
import akka.cluster.MemberStatus;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WelcomeMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private BloomFilter welcomeData;
	}

	@Data @AllArgsConstructor
	public static class HintMessage implements Serializable{
		private static final long serialVersionUID = 3303081601659723990L;
		private int userId;
		private char missingChar;
		private String encryptedHint;
		private String decryptedHint;
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class UserHint implements Serializable{
		private static final long serialVersionUID = 8343040952748609598L;
		private int userId;
		private ArrayList<String> encryptedHints;
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class GotSomeCrackBro implements Serializable{ //theres some crack
		private static final long serialVersionUID = 8343040952768609598L;
		private int userId;
		private String decryptedPassword;
	}

	@Data
	public static class TaskMessage implements Serializable{
		private static final long serialVersionUID = 1111040962748609598L;
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class CrackTaskMessage extends TaskMessage{
		private static final long serialVersionUID = 2243040962748609538L;
		private int userId;
		private String characters;
		private int passwordLength;
		private String hash;
	}
	@Data @AllArgsConstructor @NoArgsConstructor
	public static class HintTaskMessage extends TaskMessage implements Serializable{
		private static final long serialVersionUID = 8343040962748609528L;
		private String characters;
		private char missingChar;
		private ArrayList<UserHint> userHints;
	}


	@Getter	@NoArgsConstructor
	public static class FinishedTaskMessage implements Serializable{
		private static final long serialVersionUID = 8343040962749609598L;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;
	private long registrationTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(WelcomeMessage.class, this::handle)
				.match(HintTaskMessage.class, this::handle)
				.match(CrackTaskMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
			
			this.registrationTime = System.currentTimeMillis();
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void handle(WelcomeMessage message) {
		final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
		this.log().info("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in " + transmissionTime + " ms.");
	}

	private void handle(CrackTaskMessage message) {
		this.log().info("Starting CrackTask: " + message);
		String pw = "";
		ArrayList<String> permutations = new ArrayList<>();
		printAllKLengthToList(message.getCharacters().toCharArray(), message.passwordLength, permutations);

		for(String permutation : permutations){
			String hash = hash(permutation);
			if (hash.equals(message.hash)) {
				pw = hash;
				this.getContext()
						.actorSelection(this.masterSystem.address() + "/user/" + Master.DEFAULT_NAME)
						.tell(new GotSomeCrackBro(message.userId, permutation), this.self());
				break;
			}
		}

		this.getContext()
				.actorSelection(this.masterSystem.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new FinishedTaskMessage(), this.self());
}

private void handle(HintTaskMessage message) {
		this.log().info("Received HintTaskMessage");
		HashMap<String, Integer> hints = new HashMap<>();
		for(UserHint userHint : message.getUserHints()){
			for(String hash: userHint.getEncryptedHints()){
				hints.put(hash, userHint.userId);
			}
		}

		ArrayList<String> permutations = new ArrayList<>();
		heapPermutation(message.getCharacters().toCharArray(), message.getCharacters().length(), message.getCharacters().length(), permutations);

		int hintsFounds = 0;

		for(String permutation : permutations){
			String hash = hash(permutation);
			Integer user = hints.get(hash);
			if (user != null) {
				hintsFounds++;
				this.getContext()
						.actorSelection(this.masterSystem.address() + "/user/" + Master.DEFAULT_NAME)
						.tell(new HintMessage(user, message.getMissingChar(), hash, permutation), this.self());
			}
			hints.remove(hash);
		}


		this.getContext()
				.actorSelection(this.masterSystem.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new FinishedTaskMessage(), this.self());

	}

	private String hash(String characters) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}

	// inspired by https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
	static void printAllKLengthToList(char[] set, int k, List<String> list)
	{
		int n = set.length;
		printAllKLengthRec(set, "", n, k, list);
	}

	// The main recursive method
	// to print all possible
	// strings of length k
	static void printAllKLengthRec(char[] set,
								   String prefix,
								   int n, int k,List<String> list)
	{
		// Base case: k is 0,
		// print prefix
		if (k == 0)
		{
			list.add(prefix);
			return;
		}

		// One by one add all characters
		// from set and recursively
		// call for k equals to k-1
		for (int i = 0; i < n; ++i)
		{

			// Next character of input added
			String newPrefix = prefix + set[i];

			// k is decreased, because
			// we have added a new character
			printAllKLengthRec(set, newPrefix,
					n, k - 1, list);
		}
	}

}