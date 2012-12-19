package node;
import java.io.Serializable;

import util.MessageSender;
import util.NodeStatus;

// Node.java

public interface Node extends Serializable{
    String getName();

    // Node toInstance(String[] text, ID id);
    String toMessage();

    // void ackSearch();
    void ackUpdate(MessageSender sender, Node node);

    NodeStatus searchData();
    void endSearchData(NodeStatus status);

    NodeStatus updateData();
    void endUpdateData(NodeStatus status);
}
