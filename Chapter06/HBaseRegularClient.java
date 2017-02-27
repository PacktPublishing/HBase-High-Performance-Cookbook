//Regular java classes 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
//Hadoop/Hbase related classes 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseRegularClient {

  private static Configuration hbaseBconf = null;
  /**
   * Initialization the Hbase config file as static so that it remain in memory 
   * during the lifecycel of the code executions or till the time the server is 
   * is not recycled or shutdown.
   */
  static {
    hbaseBconf = HBaseConfiguration.create();
  }

  /**
   * Create a hbaseBtable in case the table is not there Else prints table already exist conf is the
   * configuration file object which needs to be passed, hbase-site.xml
   * @method creatTable
   * @inputParameters: tablename as String, ColumnFamalies as String Array[]
   * @return NA as is a voild method 
   **/
  public static void creatTable(String myHbaseBtableName, String[] cfamlies) throws Exception {
    HBaseAdmin hbaseBadmin = new HBaseAdmin(hbaseBconf);
    if (hbaseBadmin.tableExists(myHbaseBtableName)) {
      System.out.println("Ohh.. this table is already there");
    } else {
      @SuppressWarnings("deprecation")
      HTableDescriptor hbaseBtableDesc = new HTableDescriptor(myHbaseBtableName);
      for (int i = 0; i < cfamlies.length; i++) {
        hbaseBtableDesc.addFamily(new HColumnDescriptor(cfamlies[i]));
      }
      hbaseBadmin.createTable(hbaseBtableDesc);// creating a table 
      System.out.println("create table " + myHbaseBtableName + " Done.");
      // Table is created and printed with the name.
    }
  }

  /**
   * Method to delete tables @ String myHbaseBtableName as an input parameter First it disables the
   * table as Hbase Dose not allows the table to be deleted directly Delete a table
   * @method deleteTable
   * @inputParameter: table Name
   * @retunr NA as this is a void method
   **/
  public static void deleteTable(String myHbaseBtableName) {
    try {
      @SuppressWarnings("resource")
      HBaseAdmin hbaseBadmin = new HBaseAdmin(hbaseBconf);
      hbaseBadmin.disableTable(myHbaseBtableName);// this will disable the table to get any further operations 
      hbaseBadmin.deleteTable(myHbaseBtableName);// this will allow the table to be deleted 
      System.out.println("Table  delete fired : " + myHbaseBtableName + "And This is Done");
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    }catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Put (or insert) a row
   * @method addRecord
   * @inputParameter: table Name used, hbaseBrowKey, columnFamily, column hbaseBqualifier and values
   * @retunr NA as this is a void method
   */
  public static void addRecord(String myHbaseBtableName, String hbaseBrowKey, String hbaseCfamily,
      String hbaseBqualifier, String hbaseBvalue) {
    HTable hbaseBtable = null;
    try {
      hbaseBtable = new HTable(hbaseBconf, myHbaseBtableName); // adding the hbaseBtable name 
      Put put = new Put(Bytes.toBytes(hbaseBrowKey));// PUT hbaseBrowKey in bytes 
      put.add(Bytes.toBytes(hbaseCfamily), Bytes.toBytes(hbaseBqualifier), Bytes.toBytes(hbaseBvalue));
      // adding column hbaseCfamily,qualifire and values in bytes
      hbaseBtable.put(put);// PUT the above created objects to the HTable object
      System.out.println("Recored  inserted" + hbaseBrowKey + " to hbaseBtable the table: " + myHbaseBtableName + " Done.");
      // showing the inserted records
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        hbaseBtable.close(); // closing the connections in finally 
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Delete a row from an existing table 
   * @method delRecord
   * @inputParameters table Name used, hbaseBrowKey 
   * @return type: no return type as its a void method 
   * 
   **/
  public static void delRecord(String myHbaseBtableName, String hbaseBrowKey) {
    @SuppressWarnings("resource")
    HTable hbaseBtable = null;
    try {
      hbaseBtable = new HTable(hbaseBconf, myHbaseBtableName);
      List<Delete> mylist = new ArrayList<Delete>();
      Delete delKey = new Delete(hbaseBrowKey.getBytes());
      mylist.add(delKey);
      hbaseBtable.delete(mylist);
    } catch (IOException ee) {
      // 
      ee.printStackTrace();
    } finally {
      try {
        hbaseBtable.close(); // closing the connections in finally 
      } catch (IOException ep) {
        ep.printStackTrace();
      }
    }
    System.out.println("delete Recored " + hbaseBrowKey + " ok.");
  }
 /**
   * Getting a row from an existing table 
   * @method getOneRecord
   * @inputParameters table Name used, hbaseBrowKey 
   * @return type: no return type as its a void method 
   * 
   **/
  @SuppressWarnings("deprecation")
  public static void getOneRecord(String myHbaseBtableName, String hbaseBrowKey) {
    HTable hbaseBtable;
    Result hbaseBrs = null;
    try {
      hbaseBtable = new HTable(hbaseBconf, myHbaseBtableName);
      Get hbaseBget = new Get(hbaseBrowKey.getBytes());
      hbaseBrs = hbaseBtable.get(hbaseBget);
      for (KeyValue hbaseBkv : hbaseBrs.raw()) {
        System.out.print(new String(hbaseBkv.getRow()) + " ");
        System.out.print(new String(hbaseBkv.getFamily()) + ":");
        System.out.print(new String(hbaseBkv.getQualifier()) + " ");
        System.out.print(hbaseBkv.getTimestamp() + " ");
        System.out.println(new String(hbaseBkv.getValue()));
      }
    } catch (IOException eio) {
      eio.printStackTrace();
    } finally {
      if (!hbaseBrs.isEmpty()) 
      	hbaseBrs = null; // forcing the resultset to clear the hbaseBrs object in finally 
        // Change this method accrodingly if you want to use it in your code 
        // if you want to return then pls change accordingly.
    }
  }
/**
   * Getting all records  a row from an existing SS tables 
   * @method getAllRecord
   * @inputParameters hbaseBtable Name used
   * @return type: no return type as its a void method 
   * 
   **/
  @SuppressWarnings({ "deprecation", "resource" })
  public static void getAllRecord(String myHbaseBtableName) {
    ResultScanner hbaseBSs = null;
    try {
      HTable hbaseBtable = new HTable(hbaseBconf, myHbaseBtableName);
      Scan hbaseBScan = new Scan();
      hbaseBSs = hbaseBtable.getScanner(hbaseBScan);
      for (Result r : hbaseBSs) {
        for (KeyValue hbaseBkv : r.raw()) {
          System.out.print(new String(hbaseBkv.getRow()) + " ");
          System.out.print(new String(hbaseBkv.getFamily()) + ":");
          System.out.print(new String(hbaseBkv.getQualifier()) + " ");
          System.out.print(hbaseBkv.getTimestamp() + " ");
          System.out.println(new String(hbaseBkv.getValue()));
        }
      }
    } catch (IOException eio) {
      eip.printStackTrace();
    } finally {
      if (hbaseBSs != null) hbaseBSs.close();
      // closing the ss hbaseBtable 
    }
  }

  public static void main(String[] agrs) {
    try {
      String myHbaseBtableName = "mywebproject:mybrowsedata";
      String[] cfamlies = { "web", "websitedata" };
      HBaseRegularClient.creatTable(myHbaseBtableName, cfamlies);
      // add record www.google.com by calling addRecord method 
      HBaseRegularClient.addRecord(myHbaseBtableName, "www.google.com", "web", "", "best");
      HBaseRegularClient.addRecord(myHbaseBtableName, "www.google.com", "websitedata", "", "search");
      HBaseRegularClient.addRecord(myHbaseBtableName, "www.google.com", "websitedata", "gmail", "email");
      HBaseRegularClient.addRecord(myHbaseBtableName, "www.google.com", "websitedata", "google+", "hangout");
      // add record www.yahoo.com
      HBaseRegularClient.addRecord(myHbaseBtableName, "www.yahoo.com", "web", "", "secondbest");
      HBaseRegularClient.addRecord(myHbaseBtableName, "www.yahoo.com", "websitedata", "email","good email system");
      System.out.println("||===========get www.google.com record========||");
      HBaseRegularClient.getOneRecord(myHbaseBtableName, "www.google.com");
      System.out.println("||===========show all record========||");
      HBaseRegularClient.getAllRecord(myHbaseBtableName);
      System.out.println("||===========del www.yahoo.com record========||");
      HBaseRegularClient.delRecord(myHbaseBtableName, "www.yahoo.com");
      HBaseRegularClient.getAllRecord(myHbaseBtableName);
      System.out.println("||===========show all record========||");
      HBaseRegularClient.getAllRecord(myHbaseBtableName);
    } catch (Exception ep) {
      ep.printStackTrace();
    }
  }
}
