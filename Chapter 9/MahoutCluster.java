import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class MahoutClusterExample implements MahoutClusterIntrface {

  public static void main(String args[]) throws IOException {

    Configuration conf = new Configuration();
    conf.addResource(new Path(coreSiteXML));// See Interface file
    conf.addResource(new Path(hdfsSiteXML));// See Interface file 
    FileSystem fileSystem = FileSystem.get(conf);
    Path inFileDir = new Path(inpFile); // See Interface file 
    Path outFileDir = new Path(outFile);// See Interface file 
    if (!fileSystem.exists(inFileDir)) {
      System.out.println("Input file not found");
      return;
    }
    if (!fileSystem.isFile(inFileDir)) {
      System.out.println("Input should be a file");
    }
    if (fileSystem.exists(outFileDir)) {
      System.out.println("Output already exists");
      fileSystem.delete(outFileDir, true);
      System.out.println("deleted output directory");
    }
    BufferedReader bufferedReader = null;
    int counter = 0;
    int number_of_col = 0;
    try {
      bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(inFileDir)));
      String line = bufferedReader.readLine();
      @SuppressWarnings("deprecation")
      SequenceFile.Writer writer =
          new SequenceFile.Writer(fileSystem, conf, outFileDir, LongWritable.class,
              VectorWritable.class);
      
      while (line != null) {
        String[] columnDetail = line.split(" ", -1);
        double[] d = new double[columnDetail.length];
        number_of_col = columnDetail.length;
        for (int i = 0; i < columnDetail.length; i++) {
          try {
            d[i] = Double.parseDouble(columnDetail[i]);

          } catch (Exception e) {
            d[i] = 0;
          }
        }
        Vector vec = new RandomAccessSparseVector(columnDetail.length);
        vec.assign(d);
        VectorWritable writable = new VectorWritable();
        writable.set(vec);
        writer.append(new LongWritable(counter++), writable);
        line = bufferedReader.readLine();
        System.out.println("Number of lines written=" + counter);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      bufferedReader.close();
    }

    Path outputPath = new Path(clustringOut);// See interface file

    if (fileSystem.exists(outputPath)) {
      System.out.println("Output already exists");
      fileSystem.delete(outputPath, true);
      System.out.println("deleted output directory");
    }
    Path cluster_init_path = new Path(clustringInital);// See Interface file
    @SuppressWarnings("deprecation")
    SequenceFile.Writer writerClusterInitial =
        new SequenceFile.Writer(fileSystem, conf, cluster_init_path, Text.class, Kluster.class);
    for (int i = 0; i < 2; i++) {
      double[] array = new double[number_of_col];
      Arrays.fill(array, i + 1);
      Vector vec = new SequentialAccessSparseVector(number_of_col);
      vec.assign(array);
      Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
      writerClusterInitial.append(new Text(cluster.getIdentifier()), cluster);
    }
    writerClusterInitial.close();

    Path kmeans_output = new Path("clustering_output");

    if (fileSystem.exists(kmeans_output)) {
      System.out.println("Output already exists");
      fileSystem.delete(kmeans_output, true);
      System.out.println("deleted output directory");
    }
    try {
      KMeansDriver.run(conf, outFileDir, cluster_init_path, kmeans_output, 0.001, 10, true, 0,
        false);

      System.out.println("Kmeans completed");
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IndexOutOfBoundsException e) {
      System.out.println("IndexOutOfBoundsException while runnig Kmeans");
      e.printStackTrace();

    }
  }

}
