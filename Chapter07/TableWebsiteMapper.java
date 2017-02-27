import java.util.Arrays;
 // Apache hadoop related classes 
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
 
public class TableWebsiteMapper extends TableMapper<Text, IntWritable> {
	private static byte[] websiteTable =Bytes.toBytes("myTest07WebSite’"); 
    private staticbyte[] siteHitTable=Bytes.toBytes("myTest07SiteHits’"); 
    
	byte[] site;
	String website;
	Integer wsite;
	String siteHit;
	Integer csiteHit;
	Text mapperKey;
	IntWritable mapperValue;
 
	@Override
	public void map(ImmutableBytesWritable rowKey, Result columns, Context context) {
		// get table name
		TableSplit cSplit = (TableSplit)context.getInputSplit();
		byte[] tableName = cSplit.getTableName();
		try {
			if (Arrays.equals(tableName, websiteTable)) {
				String date = new String(rowKey.get()).split("#")[0];
				site = columns.getValue(Bytes.toBytes("cf02"), Bytes.toBytes("wsite"));
				website = new String(site);
				wsite = new Integer(website);
				mapperKey = new Text("s#" + date);
				mapperValue = new IntWritable(wsite);
				context.write(mapperKey, mapperValue);
			} else if (Arrays.equals(tableName, siteHitTable)) {
				String date = new String(rowKey.get());
				site = columns.getValue(Bytes.toBytes("cf01"), Bytes.toBytes("csiteHit"));
				siteHit = new String(site);
				Integer csiteHit = new Integer(siteHit);
				mapperKey = new Text("w#"+date);
				mapperValue = new IntWritable(csiteHit);
				context.write(mapperKey, mapperValue);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
