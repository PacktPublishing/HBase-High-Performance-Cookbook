/**
 * @param
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class MahoutProcessData {
  static String inputPath = "chapter10/k-meancluster-input";
  static String inputSeq = "chap10-kmean.txt";

  public static void main(String args[]) {
    try {

      Configuration conf = new Configuration();
      conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));// path for core site xml
      conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));// path for the hdfs site
                                                                         // xml
      /*
       * the config objects which has the path for both the file is passed to the new file system
       * object
       */
      FileSystem fso = FileSystem.get(conf);
      Path inputDir = new Path(inputPath);// Path for Input directory is passed
      Path inputSeqDir = new Path(inputSeq);// Path for Input sequence directory is passed
      if (fso.exists(inputSeqDir)) {
        System.out.println("Output already exists and no need to create");
        fso.delete(inputSeqDir, true);// if the dir exist it this will delete the directory.
        System.out.println("deleted output directory");
      }

      // This will encode the vectors using the //RandomAccessSparseVector
      InputDriver.runJob(inputDir, inputSeqDir, "org.apache.mahout.math.RandomAccessSparseVector",
        conf);

    } catch (ClassNotFoundException ene) {
      ene.printStackTrace();
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } catch (IOException io) {
      io.printStackTrace();
    }
  }
}
