package util;
// LocalStore.java


import java.util.ArrayList;

import node.DataNode;

public interface LocalStore {
    String getName();

    LocalStore toInstance(String[] text, ID id);
    String toMessage();

    DataNode getFirstDataNode();
    DataNode getNextDataNode(DataNode dataNode);
    ID[] getRange(DataNode dataNode);

    DataNode searchKey(ID key);
    NodeStatus searchData(DataNode dataNode);
    NodeStatus[] searchData(DataNode[] dataNodes);
    void endSearchData(NodeStatus status);

    DataNode updateKey(ID key);
    NodeStatus updateData(DataNode dataNode);
    NodeStatus[] updateData(DataNode[] dataNodes);
    void endUpdateData(NodeStatus status);

    LocalStore splitResponsibleRange(ID[] range, NodeStatus[] status);
}
