package distributedIndexTest;

import java.io.IOException;
import java.net.SocketAddress;

import util.MessageReceiver;
import util.MessageSender;

public class FakeMessageSender extends MessageSender{

	public FakeMessageSender(MessageReceiver receiver) {
		super(receiver);
	}
	
	@Override
	public String sendAndReceive(String msg, SocketAddress addr) throws IOException {
		return "user99999";
	}
}
