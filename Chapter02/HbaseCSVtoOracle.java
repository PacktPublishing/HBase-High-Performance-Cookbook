import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import au.com.bytecode.opencsv.CSVReader;
// importing the class which will be used to read the CSV file

public class HbaseCSVtoOracle implements HbaseCSVConstantsIntf {

  public HbaseCSVtoOracle() {
    // TODO Auto-generated constructor stub
  }
  static Connection conn = null;// Initializing
  static PreparedStatement sql_statement = null; // Initializing
  /**
   * @author r0choud
   * @param args
   */
  public static void main(String[] args) {
   
    try{
    Class.forName (ORACLE_DRIVER_MANAGER); // Creating a connection Object
    conn=DriverManager.getConnection(THIN_DRIVER_HOST_PORT, ORCALE_USER, ORCALE_PASSWORD);
    /*
     * using s oracle thin driver connecting to the host "your host"
     * using the port as 1512
     * user scott -> please change accordingly 
     * pasword tiget --> please change accordingly 
     */
    
    /* Create the insert statement which will insert the data to the above created table by 
     * getting the data from the CSV file
     * this can be f
     * */
   
    sql_statement = conn.prepareStatement(JDBC_INSERT_SQL); // passing sql query string to the preparedStatement.
    /* Read CSV file in OpenCSV */
    @SuppressWarnings("resource")
    CSVReader reader = new CSVReader(new FileReader(INPUT_CSVFILE_LOCATON));
    /* Variables to loop through the CSV File */
    String [] nextLine; /* for every line in the file */            
    @SuppressWarnings("unused")
    int lnNum = 0; /* line number */
    while ((nextLine = reader.readNext()) != null) {
            lnNum++;
            /* Bind CSV file input to table columns */
            sql_statement.setString(1, nextLine[0]);
            /* Bind Age as double */
            /* Need to convert string to double here */
            sql_statement.setDouble(2,Double.parseDouble(nextLine[1]));
            /* execute the insert statement */
            sql_statement.executeUpdate();
    }               
    
    }catch(Exception e)
    {
      e.printStackTrace();
    }finally
    {
        closeConnection(); 
    }
    
    
}

  private static void closeConnection() {
      /* Close prepared statement */
      try {
        sql_statement.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      /* COMMIT transaction */
      try {
        conn.commit();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      /* Close connection */
      try {
        conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      
  }

  } 
