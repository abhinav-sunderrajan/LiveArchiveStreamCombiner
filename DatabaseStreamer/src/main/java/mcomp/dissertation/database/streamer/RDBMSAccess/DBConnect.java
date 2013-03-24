package mcomp.dissertation.database.streamer.RDBMSAccess;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

/**
 * The class responsible for handling database operations.
 */
public class DBConnect {

   private Connection connect = null;
   private static final Logger LOGGER = Logger.getLogger(DBConnect.class);
   private static final String TABLE_NAME = "DataArchive";
   private static final String SELECT_QUERY = "SELECT LINKID,SPEED,VOLUME,TIME_STAMP FROM "
         + TABLE_NAME
         + " WHERE TIME_STAMP >= ? AND TIME_STAMP< ? ORDER BY LINKID";

   /**
    * @param connectionProperties
    */
   public void openDBConnection(final Properties connectionProperties) {
      if (connectionProperties.getProperty("database.vendor").equalsIgnoreCase(
            "MySQL")) {

         String url = "jdbc:mysql://localhost:3306/";
         String dbName = connectionProperties.getProperty("database.name");
         String driver = "com.mysql.jdbc.Driver";
         String userName = connectionProperties
               .getProperty("database.username");
         String password = connectionProperties
               .getProperty("database.password");
         try {
            Class.forName(driver).newInstance();
            connect = (Connection) DriverManager.getConnection(url + dbName,
                  userName, password);
            LOGGER.info("Connected to "
                  + connectionProperties.getProperty("database.vendor"));

         } catch (Exception e) {
            LOGGER.error(
                  "Unable to connect to database. Please check the settings", e);
         }

      }
   }

   /**
    * @param start
    * @param end
    * @return ResultSet
    * @throws SQLException
    */
   public ResultSet retrieveWithinTimeStamp(final Timestamp start,
         final Timestamp end) throws SQLException {
      ResultSet rs = null;
      PreparedStatement preparedStatement = (PreparedStatement) connect
            .prepareStatement(SELECT_QUERY);
      try {
         preparedStatement.setTimestamp(1, start);
         preparedStatement.setTimestamp(2, end);
         rs = preparedStatement.executeQuery();
         LOGGER.info("Fetched records between " + start + " and " + end);
      } catch (SQLException e) {
         LOGGER.error("Unable to retreive records", e);

      }
      return rs;
   }

   /**
    * 
    * @param timestamps
    * @return ResultSet
    * @throws SQLException
    */
   public ResultSet retrieveAggregates(final Timestamp[] timestamps)
         throws SQLException {
      ResultSet rs = null;
      StringBuffer temp = new StringBuffer("");
      for (int count = 0; count < timestamps.length; count++) {
         if (count == (timestamps.length - 1)) {
            temp.append("?");
         } else {
            temp.append("?,");
         }
      }
      String aggregateQuery = "SELECT LINKID,AVG(SPEED),AVG(VOLUME) FROM "
            + TABLE_NAME + " WHERE TIME_STAMP IN(" + temp + ") GROUP BY LINKID";
      PreparedStatement preparedStatement = (PreparedStatement) connect
            .prepareStatement(aggregateQuery);
      try {
         for (int count = 0; count < timestamps.length; count++) {
            preparedStatement.setTimestamp(count + 1, timestamps[count]);
         }
         rs = preparedStatement.executeQuery();
         LOGGER.info("Fetched records between " + timestamps[0] + " and "
               + timestamps[timestamps.length - 1]);
      } catch (SQLException e) {
         LOGGER.error("Unable to retreive records", e);

      }
      return rs;

   }
}
