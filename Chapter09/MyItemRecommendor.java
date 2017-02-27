import java.io.File;
import java.io.IOException;
import java.util.List;
//Mahout specicfic classes will require all the lib  as specified 
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
public class ItemRecommender {

  public ItemRecommender() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      DataModel dm = new FileDataModel(new File("data/movies.csv"));
      
      //ItemSimilarity sim = new LogLikelihoodSimilarity(dm);
      TanimotoCoefficientSimilarity sim = new TanimotoCoefficientSimilarity(dm);
      GenericItemBasedRecommender recommender = new GenericItemBasedRecommender(dm, sim);
      
      int x=1;
      for(LongPrimitiveIterator items = dm.getItemIDs(); items.hasNext();) {
        long itemId = items.nextLong();
        List<RecommendedItem>recommendations = recommender.mostSimilarItems(itemId, 6);/* recommendation based on similarity, count it 6 , this can be changed as desired */
        
        for(RecommendedItem recommendation : recommendations) {
          System.out.println(itemId + "," + recommendation.getItemID() + "," + recommendation.getValue());
        }
        x++;
        //if(x>10) System.exit(1);
      }
            
      
      
    } catch (IOException e) {
      System.out.println("There was an error.");
      e.printStackTrace();
    } catch (TasteException e) {
      System.out.println("There was a Taste Exception");
      e.printStackTrace();
    }
    

  }

}
