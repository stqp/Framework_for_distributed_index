package distributedIndex;
// DistributedIndex.java


import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import loadBalance.LoadInfoTable;

import util.ID;
import util.MessageSender;
import util.NodeStatus;

import node.AddressNode;
import node.DataNode;
import node.Node;

public interface DistributedIndex {
    String getName();


    /*
     * 他の計算機から送られてきたメッセージを処理します。
     */
    String handleMessge(InetAddress host, ID id, String[] text);


    /*
     * 分散インデックス手法の状態を初期化する
     */
    void initialize(ID id);
    void initialize(DistributedIndex distIndex, InetSocketAddress addr, ID id);
    DistributedIndex toInstance(String[] text, ID id);


    String toMessage();


    //よりわからないですが徳田は使っていないメソッドです。
    InetSocketAddress[] getAckMachine();
    String toAdjustInfo();
    AddressNode adjust(String text, ID id, InetSocketAddress addr, String info);



    //自分が担当するIDの始まりを返します。たぶん。
    ID getID();

    /*
     * 自分の担当するID範囲を返します。
     * SkipGraphなどでは自分の最後のIDがわからないので
     * MessageSenderを使って隣の計算機と通信します。
     */
    ID[] getResponsibleRange(MessageSender sender) throws IOException;


    /*
     * 自分の担当する範囲のうち最小のIDを含むデータノードを返します。
     */
    DataNode getFirstDataNode();


    /*
     * 渡したデータノードの次にあたるデータノードを返します。
     */
    DataNode getNextDataNode(DataNode dataNode);


    /*
     *
     */
    ID[] getDataNodeRange(DataNode dataNode);


    /*
     * 自分に対して次にあたる計算機のアドレスを返します。
     * つまりこのフレームワークは計算機が「次」という概念の接続関係にあること
     * を前提としています。
     *
     * 例えば、自分に対して「次」にあたる計算機がない場合はこのフレームワークは使えない。
     * （ハッシュを使って計算機が接続されているときなど？）
     */
    InetSocketAddress getNextMachine();


    /*
     * 単一のキーに対して検索をかける
     */
    Node searchKey(MessageSender sender, ID key) throws IOException;


    /*
     * よくわからないが、単一のキーに対して検索を行う
     * そしてDataNodeを返す時はそのノードがキーを含むとき
     * AddressNodeを返す時は転送先にキーがある可能性があるとき。
     * 
     * textには 「_first_」 だったり、何か保存用ハッシュのキーだったりと、
     * 使い方が悪いように思える。
     * そもそも設計自体がおかしいように思える。
     * 
     */
    Node searchKey(MessageSender sender, ID key, String text) throws IOException;

    // Node searchKey(MessageSender sender, ID key, Node start) throws IOException;


    /*
     * 読み込み用
     * rangeで指定した範囲のキーをもつデータノードの（たぶんラッチ）状態を配列にして返す。っぽい。
     */
    NodeStatus[] searchData(MessageSender sender, ID[] range) throws IOException; // TODO: to algorithm (now use status command)


    /*
     * 読み込み用
     * 渡したデータノードの（たぶんラッチ）状態を返す。っぽい。
     */
    NodeStatus searchData(MessageSender sender, DataNode dataNode) throws IOException;


    /*
     * 読み込み用
     * 渡したデータノードの配列の（たぶんラッチ）状態を返す。っぽい。
     */
    NodeStatus[] searchData(MessageSender sender, DataNode[] dataNodes) throws IOException;


    /*
     * ステータスの配列のオーナーにあたるノードの（たぶんラッチ）状態をもとに戻すっぽい。
     */
    void endSearchData(MessageSender sender, NodeStatus status[]) throws IOException;


    /*
     * ステータスのオーナー(owner。@see -> NodeStatus.java definition)にあたるノードの（たぶんラッチ）状態をもとに戻すっぽい。
     */
    void endSearchData(MessageSender sender, NodeStatus status) throws IOException;



    /*
     * 書き込み用
     * よくわからないな
     */
    Node updateKey(MessageSender sender, ID key) throws IOException;


    /*
     * 書き込み用
     * おなじくよくわからない
     */
    Node updateKey(MessageSender sender, ID key, String text) throws IOException;


    /*
     * 書き込み用
     */
    NodeStatus[] updateData(MessageSender sender, ID[] range) throws IOException; // TODO: to algorithm (now use init command)


    /*
     * 書き込み用
     */
    NodeStatus updateData(MessageSender sender, DataNode dataNode) throws IOException;


    /*
     * 書き込み用
     */
    NodeStatus[] updateData(MessageSender sender, DataNode[] dataNodes) throws IOException;


    /*
     * 書き込み終了用
     * たぶん書き込んでいたデータノードの状態を元に戻す
     */
    void endUpdateData(MessageSender sender, NodeStatus[] status);


    /*
     * 書き込み終了用
     */
    void endUpdateData(MessageSender sender, NodeStatus status);


    /*
     * 新しく参加する計算機のためにIDを作成します。
     */
    ID[] getRangeForNew(ID id);


    /*
     * 担当範囲をスプリットして新しく分散インデクス手法を返します。
     */
    DistributedIndex splitResponsibleRange(MessageSender sender, ID[] range, ID id, NodeStatus[] status, InetSocketAddress addr);



    /*
     * 負荷分散のためのメソッドだったらしいです。
     *  boolean detectSkew();
     */

    boolean adjustCmd(MessageSender sender) throws IOException;
    String getAdjustCmdInfo() throws IOException;






    /*
     * 自分のコンピュータアドレスをセットします。
     * 分散インデックス手法をインスタンス化した時にはまだ
     * アドレスが取れないのでこのメソッドを作成しました。
     */
    void setMyAddress(InetSocketAddress address);

    /*
     * 自分のコンピュータアドレスを返します。
     * 負荷情報を他の計算機と転送しあうので、自分の負荷を判別するために使用します。
     */
    public InetSocketAddress getMyAddress();



    /*
     * アドレスをIPだけにして返します。
     */
    public String getMyAddressIPString();



    /*
     * データ容量偏り（まだできていないが、アクセス負荷）の調査及び、
     * 負荷が大きい場合にはデータ移動
     * そしてインデクス更新
     * の操作を行います。
     */
    public void checkLoad(LoadInfoTable loadInfoTable, MessageSender sender);
    void moveData(DataNode[] dataNodesToBeRemoved,InetSocketAddress target, MessageSender sender );




    /*
     * データ移動に伴って生じるインデクス更新によって、ほかの計算機から更新情報が送られてくるので
     * それを自分のインデクスに適用します。
     */
    public String recieveAndUpdateDataForLoadMove(DataNode[] dataNodes, InetSocketAddress senderAddress);




    /*
     * moveLeftmostDataNodes
     * moveRightmostDataNodes
     * addPassedDataNodes
     * のメソッドは使いません。
     */
    //public DataNode[] moveLeftmostDataNodes(DataNode[] dataNodesToBeRemoved, InetSocketAddress address, MessageSender sender);
   // public DataNode[] moveRightmostDataNodes(DataNode[] dataNodesToBeRemoved, InetSocketAddress address, MessageSender sender);
   // public void addPassedDataNodes(boolean toLeft, List<DataNode> dataNodes);
}
















