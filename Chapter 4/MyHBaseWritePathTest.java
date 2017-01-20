import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseWritePathTest throws MasterNotRunningException, ZooKeeperConnectionException 
{
    {
        public static void main(String[] args) throws IOException {
            try {
                HBaseAdmin.checkHBaseAvailable(conf);// checking for the config file 
            } catch (Exception e) {
                System.err.println("Exception at " + e);
                System.exit(1);
            }
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "infinity");//setting zookeepers properity
            conf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeprs port on which zookeepr is listening.
            HTable table = new HTable(conf, "MyWriretestTable");// passing the table detakls 
            Put p = new Put(Bytes.toBytes("myWriteTestRow")); // passing the row which needs to be written in bytes
            p.add(Bytes.toBytes("myWriteTestFamily"), Bytes.toBytes("myWriteTestQualifier"),// passing column qualifier 
            Bytes.toBytes("myWriteTest"));
            HBaseAdmin admin = new HBaseAdmin(conf); // passing the configuraiton 
            try {
                HBaseAdmin.checkHBaseAvailable(conf);
                System.out.println("Test for success ! ");
            } catch (Exception error) {
                System.err.println("Error connecting HBase:  " + error);System.exit(1);
            }
        }
    }
 //Now you can run the command and you will see Test got success ! in you Eclipse logs. 
 //Note this code traverses the entire write path of HBASE 
 // You can check by adding a scan method in the above code and invoke it once the insert is done.
 //Alternatively you can issues a direct command as scan table ‘MyWriretestTable’ on hbase console. 
 /**
     * Scan for table, this needs to be passed as a parameter
 */
    public static void getAllRecord (String tableName) {
    ResultScanner ss= null; // initializing to to null
        try{
             HTable table = new HTable(conf, MyWriretestTable); // passing the configuraiotn and table Nane
             Scan s = new Scan(); // creating a Scan objet
             ss = table.getScanner(s);// actually invoking the get methoud for Scan
             for(Result r:ss){
                 for(KeyValue kv : r.raw())
                { 
                    System.out.print(new String(kv.getRow()) + " ");// printing row
                    System.out.print(new String(kv.getFamily()) + ":");// printing column
                    System.out.print(new String(kv.getQualifier()) + " "); // priniting Qualifier 
                    System.out.print(kv.getTimestamp() + " "); // getting kv timestamp
                    System.out.println(new String(kv.getValue())); // getting the value 
                 }
             }
        } catch (IOException e){
            e.printStackTrace(); // Printing stacktrace in case of I/O error
        }finally 
        { 
        ss.close();
        } // Finally closing the Result Scanner in fiablly 
    }
 }

