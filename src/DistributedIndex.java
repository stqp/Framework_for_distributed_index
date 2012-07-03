// DistributedIndex.java

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public interface DistributedIndex {
    String getName();

    String handleMessge(InetAddress host, ID id, String[] text);

    void initialize(ID id);
    void initialize(DistributedIndex distIndex, InetSocketAddress addr, ID id);
    DistributedIndex toInstance(String[] text, ID id);
    String toMessage();
    String toAdjustInfo();
    AddressNode adjust(String text, ID id, InetSocketAddress addr, String info);
    InetSocketAddress[] getAckMachine();

    ID getID();
    ID[] getResponsibleRange(MessageSender sender) throws IOException;
    DataNode getFirstDataNode();
    DataNode getNextDataNode(DataNode dataNode);
    ID[] getDataNodeRange(DataNode dataNode);
    InetSocketAddress getNextMachine();
    // InetSocketAddress getPrevMachine();

    Node searchKey(MessageSender sender, ID key) throws IOException;
    Node searchKey(MessageSender sender, ID key, String text) throws IOException;
    // Node searchKey(MessageSender sender, ID key, Node start) throws IOException;
    NodeStatus[] searchData(MessageSender sender, ID[] range) throws IOException; // TODO: to algorithm (now use status command)
    NodeStatus searchData(MessageSender sender, DataNode dataNode) throws IOException;
    NodeStatus[] searchData(MessageSender sender, DataNode[] dataNodes) throws IOException;
    void endSearchData(MessageSender sender, NodeStatus status[]) throws IOException;
    void endSearchData(MessageSender sender, NodeStatus status) throws IOException;

    Node updateKey(MessageSender sender, ID key) throws IOException;
    Node updateKey(MessageSender sender, ID key, String text) throws IOException;
    // Node updateKey(MessageSender sender, ID key, Node start) throws IOException;
    NodeStatus[] updateData(MessageSender sender, ID[] range) throws IOException; // TODO: to algorithm (now use init command)
    NodeStatus updateData(MessageSender sender, DataNode dataNode) throws IOException;
    NodeStatus[] updateData(MessageSender sender, DataNode[] dataNodes) throws IOException;
    void endUpdateData(MessageSender sender, NodeStatus[] status);
    void endUpdateData(MessageSender sender, NodeStatus status);

    ID[] getRangeForNew(ID id);
    DistributedIndex splitResponsibleRange(MessageSender sender, ID[] range, ID id, NodeStatus[] status, InetSocketAddress addr);

    // LoadInfo getLoadInfo();
    // boolean detectSkew();

    // 
    boolean adjustCmd(MessageSender sender) throws IOException;
    String getAdjustCmdInfo() throws IOException;
}
