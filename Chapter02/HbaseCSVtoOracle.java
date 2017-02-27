import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import au.com.bytecode.opencsv.CSVReader;
// importing the class which will be used to read the CSV file

public class HbaseCSVtoOracle {

  public HbaseCSVtoOracle() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @author r0choud
   * @param args
   */
  public static void main(String[] args) {
    Connection conn = null;
    PreparedStatement sql_statement = null; // Initializing
    try{
    final String inputCSVFile = "/u/HbaseB/WDI_Country.csv"; //getting the CSV file from the location we have saved.
    Class.forName ("oracle.jdbc.OracleDriver"); // Creating a connection Object
    conn=DriverManager.getConnection("jdbc:oracle:thin:@//myoracle.db.com:1521/xe", "scott", "tiger");
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
    final String jdbc_insert_sql = "INSERT INTO WDI_COUNTRY" +"( "
                    + "(COUNTRY_CODE,SHORT,TABLE_NAME,ALPHA_CODE,CURRENCY_UNIT,SPECIAL_NOTES,REGION,)"
                    + "(INCOME_GROUP,INTERNATIONAL_MEMBERSHIPS,WB_2_CODE,NATIONAL_ACCOUNTS_BASE_YEAR,)"
                    + "(NATIONAL_ACCOUNTS_REF_YEAR,SNA_PRICE_VALUATION,LENDING_CATEGORY,OTHER_GROUPS,)"
                    + "(SYSTEM_OF_NATIONAL_ACCOUNTS,ALTERNATIVE_CONVERSION_FACTOR,PPP_SURVEY_YEAR_BALANCE,)"
                    + "(EXTERNAL_DEBT_REPORT_STATUS,SYSTEM_OF_TRADE,GOVERNMENT_ACCT_CONCEPT,IMF_DATA_DISSEMINATION_STD,)"
                    + "(LATEST_POPULATION_CENSUS,LATEST_HOUSEHOLD_SURVEY,SOURCE_OF_MOST_RECENT_INCOME,VITAL_REGISTRATION_COMPLETE,)"
                    + "(LATEST_AGRICULTURAL_CENSUS,LATEST_INDUSTRIAL_DATA.LATEST_TRADE_DATA,LATEST_WATER_WITHDRAWAL_DATA,)"+")"
                    + "VALUES"
                    + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    sql_statement = conn.prepareStatement(jdbc_insert_sql); // passing sql query string to the preparedStatement.
    /* Read CSV file in OpenCSV */
    @SuppressWarnings("resource")
    CSVReader reader = new CSVReader(new FileReader(inputCSVFile));
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

  } 
