package node;
import util.MessageSender;
import util.NodeStatus;

// Node.java

public interface Node {
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
