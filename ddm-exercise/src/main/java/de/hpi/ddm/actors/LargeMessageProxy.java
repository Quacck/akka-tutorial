package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

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

	/////////////////
	// Actor State //
	/////////////////

	ArrayList<byte[]> receiveBuffer = new ArrayList<>();
	
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
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> largeMessage) {
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		byte[] array = KryoPoolSingleton.get().toBytesWithClass(message);
		System.out.println(message);

		ArrayList<byte[]> parts = new ArrayList<>();

		int chunk = 10; // chunk size to divide
		for(int i=0;i<array.length;i+=chunk){
			parts.add(Arrays.copyOfRange(array, i, Math.min(array.length,i+chunk)));
		}

		for (int i = 0; i < parts.size(); i++){
			// System.out.println(Arrays.toString(parts.get(i)));
			boolean isLast = (i == parts.size() - 1);
			receiverProxy.tell(new BytesMessage<byte[]>(parts.get(i), isLast, sender, receiver), this.self());
		}

		// TODO: Implement a protocol that transmits the potentially very large message object.
		// The following code sends the entire message wrapped in a BytesMessage, which will definitely fail in a distributed setting if the message is large!
		// Solution options:
		// a) Split the message into smaller batches of fixed size and send the batches via ...
		//    a.a) self-build send-and-ack protocol (see Master/Worker pull propagation), or
		//    a.b) Akka streaming using the streams build-in backpressure mechanisms.
		// b) Send the entire message via Akka's http client-server component.
		// c) Other ideas ...
		// Hints for splitting:
		// - To split an object, serialize it into a byte array and then send the byte array range-by-range (tip: try "KryoPoolSingleton.get()").
		// - If you serialize a message manually and send it, it will, of course, be serialized again by Akka's message passing subsystem.
		// - But: Good, language-dependent serializers (such as kryo) are aware of byte arrays so that their serialization is very effective w.r.t. serialization time and size of serialized data.
		// receiverProxy.tell(new BytesMessage<>(message, sender, receiver), this.self());
	}

	private void handle(BytesMessage<?> message) {
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		// System.out.println(Arrays.toString((byte[])message.bytes));
		this.receiveBuffer.add((byte[])message.bytes);

		if (message.isLast) {
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
			Object decodedMessage = KryoPoolSingleton.get().fromBytes(fullmessage);
			System.out.println(decodedMessage);
			message.getReceiver().tell(decodedMessage, message.getSender());
		}
	}
}
