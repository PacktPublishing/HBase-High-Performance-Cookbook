package Col;
// Please change the package accordingly 
//Java IO class
import java.io.IOException;
// Hbase specific class 
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;

public class RestHbaseClent {

  private static RemoteHTable table;//  
  /* has three Constructurs we are using
   * RemoteHTable(Client client, String name)
   * https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/rest/client/RemoteHTable.html
  */
  public static void main(String[] args) throws Exception {
    getDataFromHbaseRest();
  }

  public static void getDataFromHbaseRest() {
    ResultScanner scanner = null;// it needs to be initialized to null
    Cluster hbaseCluster = new Cluster();//Creating and cluster object
    hbaseCluster.add("172.28.182.45", 8080);//passing the IP and post 
    // Create Rest client instance and get the connection
    Client restClient = new Client(hbaseCluster);//pass the cluster object to the cliet
    table = new RemoteHTable(restClient, "mywebproject:myclickstream");// Makes a Remote Call 
    Get get = new Get(Bytes.toBytes("row02"));//Gets the row in question
    Result result1=null;// initilizing it to null
    try {
      result1 = table.get(get);// getting the table and the connection object
      byte[] valueWeb = result1.getValue(Bytes.toBytes("web"), Bytes.toBytes("col01"));
      byte[] valueWeb01 = result1.getValue(Bytes.toBytes("web"), Bytes.toBytes("col02"));
      /*
       * getting the  colum family: column qualifire values 
       * */
      byte[] valueWebData = result1.getValue(Bytes.toBytes("websitedata"), Bytes.toBytes("col01"));
      byte[] valueWebData01 = result1.getValue(Bytes.toBytes("websitedata"), Bytes.toBytes("col02"));
      /*
       * getting the  colum family: column qualifire values 
       * */
      String valueStr = Bytes.toString(valueWeb);
      String valueStr1 = Bytes.toString(valueWeb01);
      String valueWebdataStr = Bytes.toString(valueWebData);
      String valueWebdataStr1 = Bytes.toString(valueWebData01);
      
      System.out.println("GET: \n" + " web: " + valueStr + "\n web: " + valueStr1+"\n "+"Webdata: "+valueWebdataStr);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }finally{
      /*make sure the resultset is set to null befoer exiting the program
       * In case its needed keep the object, but whenever the object is removed from the
       * rs, please null it. Its a good programming practive.
      */
      if(!result1.isEmpty());
      result1=null;
    }
    ResultScanner rsScanner = null;
    try {
      Scan s = new Scan();
      s.addColumn(Bytes.toBytes("web"), Bytes.toBytes("col01"));
      s.addColumn(Bytes.toBytes("web"), Bytes.toBytes("col02"));
      rsScanner = table.getScanner(s);

      for (Result rr = rsScanner.next(); rr != null; rr = rsScanner.next()) {
        System.out.println("Found row : " + rr);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Make sure you close your scanners when you are done!
      rsScanner.close();
    }
  }
