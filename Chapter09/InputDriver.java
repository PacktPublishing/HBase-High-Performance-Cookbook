import java.io.IOException;
	 
	import org.apache.mahout.clustering.conversion.InputMapper;
	import org.apache.commons.cli2.CommandLine;
	import org.apache.commons.cli2.Group;
	import org.apache.commons.cli2.Option;
	import org.apache.commons.cli2.OptionException;
	import org.apache.commons.cli2.builder.ArgumentBuilder;
	import org.apache.commons.cli2.builder.DefaultOptionBuilder;
	import org.apache.commons.cli2.builder.GroupBuilder;
	import org.apache.commons.cli2.commandline.Parser;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
	import org.apache.mahout.common.CommandLineUtil;
	import org.apache.mahout.common.commandline.DefaultOptionCreator;
	import org.apache.mahout.math.VectorWritable;
	import org.slf4j.Logger;
	import org.slf4j.LoggerFactory;
	 
	/**
	 * This class converts text files containing space-delimited floating point numbers into
	 * Mahout sequence files of VectorWritable suitable for input to the clustering jobs in
	 * particular, and any Mahout job requiring this input in general.
	 *
	 */
	public final class InputDriver {
	 
	  private static final Logger log = LoggerFactory.getLogger(InputDriver.class);
	  
	  private InputDriver() {
	  }
	  
	  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	    DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    
	    Option inputOpt = DefaultOptionCreator.inputOption().withRequired(false).create();
	    Option outputOpt = DefaultOptionCreator.outputOption().withRequired(false).create();
	    Option vectorOpt = obuilder.withLongName("vector").withRequired(false).withArgument(
	      abuilder.withName("v").withMinimum(1).withMaximum(1).create()).withDescription(
	      "The vector implementation to use.").withShortName("v").create();
	    
	    Option helpOpt = DefaultOptionCreator.helpOption();
	    
	    Group group = gbuilder.withName("Options").withOption(inputOpt).withOption(outputOpt).withOption(
	      vectorOpt).withOption(helpOpt).create();
	 
	    try {
	      Parser parser = new Parser();
	      parser.setGroup(group);
	      CommandLine cmdLine = parser.parse(args);
	      if (cmdLine.hasOption(helpOpt)) {
	        CommandLineUtil.printHelp(group);
	        return;
	      }
	      
	      Path input = new Path(cmdLine.getValue(inputOpt, "testdata").toString());
	      Path output = new Path(cmdLine.getValue(outputOpt, "output").toString());
	      String vectorClassName = cmdLine.getValue(vectorOpt,
	         "org.apache.mahout.math.RandomAccessSparseVector").toString();
	      //runJob(input, output, vectorClassName);
	    } catch (OptionException e) {
	      InputDriver.log.error("Exception parsing command line: ", e);
	      CommandLineUtil.printHelp(group);
	    }
	  }
	  
	  public static void runJob(Path input, Path output, String vectorClassName,Configuration config)
	    throws IOException, InterruptedException, ClassNotFoundException {
	    Configuration conf = config;
	    conf.set("vector.implementation.class.name", vectorClassName);
	    Job job = new Job(conf, "Input Driver running over input: " + input);
	 
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(VectorWritable.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setMapperClass(InputMapper.class);   
	    job.setNumReduceTasks(0);
	    job.setJarByClass(InputDriver.class);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    job.waitForCompletion(true);
	  }
	  
}

