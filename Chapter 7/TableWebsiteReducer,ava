 pakage com.hbasebook.chap07.mywebsite.WebSiteMapReduceJob
import java.io.IOException;
 // Apache hadoop+hbase related classes 
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
 
public class TableWebsiteReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{
	 @Override
	 public void reduce(Text key, Iterable<IntWritable> values, Context context)  {
		 if (key.toString().startsWith("s")) {
			 Integer wsiteVisit = 0;
			 for (IntWritable website : values) {
				 wsiteVisit = wsiteVisit + new Integer(website.toString());
			 }
			 Put put = new Put(Bytes.toBytes(key.toString()));
			 put.add(Bytes.toBytes("cf02"), Bytes.toBytes("twebsite"), Bytes.toBytes(wsiteVisit));
			 try {
				context.write(null, put);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		 } else {// going for site hit numbers
			  Integer wsiteVisit = 0;
			 for (IntWritable website : values) {
				 wsiteVisit = wsiteVisit + new Integer(website.toString());
			 }
			 Put put = new Put(Bytes.toBytes(key.toString()));
			 put.add(Bytes.toBytes("cf02"), Bytes.toBytes("twebsite"), Bytes.toBytes(wsiteVisit));
			 try {
				context.write(null, put);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		 }
	 }
}
