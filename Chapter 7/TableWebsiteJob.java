com.hbasebook.chap07.mywebsite.WebSiteMapReduceJob
import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
 
public class TableWebsiteJob extends Configured implements Tool {
 
	@Override
	public int run(String[] arg0) throws Exception {
		List<Scan> mainSiteScan = new ArrayList<Scan>();
		Scan siteScan = new Scan();
		siteScan.setAttribute("scan.attributes.table.name", Bytes.toBytes("myTest07WebSite"));
		System.out.println(siteScan.getAttribute("scan.attributes.table.name"));
		mainSiteScan.add(siteScan);
 
		Scan webSitehitScan = new Scan();
		webSitehitScan.setAttribute("scan.attributes.table.name", Bytes.toBytes("myTest07SiteHits"));// lookup for the table which we have created and is having the site hit data.
		System.out.println(webSitehitScan.getAttribute("scan.attributes.table.name"));
		mainSiteScan.add(webSitehitScan);
 
		Configuration conf = new Configuration();
		Job job = new Job(conf);
// will get the server details of Hbase/hadoop	
		job.setJarByClass(TableWebsiteJob.class);
 // setting the class name to the job
		TableMapReduceUtil.initTableMapperJob(
				mainSiteScan, // tables to read from 
				TableWebsiteMapper.class, 
				Text.class, 
				IntWritable.class, 
				job);
	    TableMapReduceUtil.initTableReducerJob(
	    		"myTest07SiteHitsPlusWebSite",
	    		TableWebsiteReducer.class, 
	    		job);
	    job.waitForCompletion(true);
		return 0;
// totalhit is the third table which will receive the data
	}
	
	public static void main(String[] args) throws Exception {
		TableWebsiteJob runWebHitJob = new TableWebsiteJob();
		runWebHitJob.run(args);
// invoking the class 
	}
}
