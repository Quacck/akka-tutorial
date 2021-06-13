package de.hpi.ddm.actors;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.javadsl.*;
import com.esotericsoftware.minlog.Log;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// our solution was heavily inspired by  https://doc.akka.io/docs/akka/current/stream/operators/Sink/actorRefWithBackpressure.html
public class LargeMessageProxy extends AbstractLoggingActor {

	int CHUNKYNESS = 1024; // chunk size to divide

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private boolean isLast;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor
	static class StreamInitialized implements Serializable {
		private static final long serialVersionUID = 5057807743872319842L;
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	static class StreamCompleted implements Serializable
	{
		private static final long serialVersionUID = 6057807777872319842L;
		ActorRef receiver;
	}

	@Data
	static class StreamFailure implements Serializable {
		private static final long serialVersionUID = 9057807743889819842L;
		private final Throwable cause;

		public StreamFailure(Throwable cause) {
			this.cause = cause;
		}
	}

	/////////////////
	// Actor State //
	/////////////////

	ArrayList<byte[]> receiveBuffer = new ArrayList<>();
	enum Ack {
		INSTANCE;
	}
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(StreamInitialized.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.match(StreamCompleted.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> largeMessage) throws ExecutionException, InterruptedException {
		this.log().info("Received Large Message. From {}", sender());
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		try {
			ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
			ActorRef receiverProxyRef = receiverProxy.resolveOne(Duration.ofSeconds(5)).toCompletableFuture().get();
			byte[] array = KryoPoolSingleton.get().toBytesWithClass(message);

			ArrayList<BytesMessage<byte[]>> parts = new ArrayList<>();

			for(int i=0;i<array.length;i += CHUNKYNESS){
				parts.add(new BytesMessage<byte[]>(Arrays.copyOfRange(array, i, Math.min(array.length,i+CHUNKYNESS)), false, sender, receiver));
			}
			Source<BytesMessage<byte[]>, NotUsed> source = Source.from(parts);
			Sink<BytesMessage<byte[]>, NotUsed> sink = Sink.<BytesMessage<byte[]>>actorRefWithBackpressure(receiverProxyRef, new StreamInitialized(), Ack.INSTANCE, new StreamCompleted(receiver), StreamFailure::new);
			source.runWith(sink, this.getContext().getSystem());
		} catch (Exception e){
			this.sender().tell(largeMessage, self());
		}
	}

	private void handle(StreamCompleted complete){
		this.log().info("Sent LargeMessage" +  complete.getReceiver());
		int size = 0;
		for (byte[] part : this.receiveBuffer) {
			size += part.length;
		}

		byte[] fullmessage = new byte[size];

		int currentPos = 0;
		for (byte[] part : this.receiveBuffer) {
			System.arraycopy(part, 0, fullmessage, currentPos, part.length);
			currentPos += part.length;
		}

		// this.log().info(Arrays.toString(fullmessage));

		Object decodedMessage = KryoPoolSingleton.get().fromBytes(fullmessage);
		complete.getReceiver().tell(decodedMessage, getSelf());
	}

	private void handle(StreamInitialized initialized) {
		this.receiveBuffer.clear();
		sender().tell(Ack.INSTANCE, self());
	}

	private void handle(BytesMessage<?> message) {
		this.receiveBuffer.add((byte[]) message.bytes);
		sender().tell(Ack.INSTANCE, self());
	}
}
