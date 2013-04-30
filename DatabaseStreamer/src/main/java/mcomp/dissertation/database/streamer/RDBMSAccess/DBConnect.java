package mcomp.dissertation.database.streamer.RDBMSAccess;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import mcomp.dissertation.helper.CommonHelper;

import org.apache.log4j.Logger;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

/**
 * The class responsible for handling database operations.
 */
public class DBConnect {

   private Connection connect = null;
   private Object lock;
   private int oddOrEven;
   private static CommonHelper helper;
   private static final Logger LOGGER = Logger.getLogger(DBConnect.class);
   private static final String TABLE_NAME = "dataarchive";
   private static final String SELECT_QUERY = "SELECT LINKID,SPEED,VOLUME,TIME_STAMP FROM "
         + TABLE_NAME
         + " WHERE TIME_STAMP >= ? AND TIME_STAMP< ? ORDER BY LINKID";

   /**
    * The lock for synchronization of ping and retrieve and records
    * @param lock
    */
   public DBConnect(Object lock) {
      this.lock = lock;
      oddOrEven = -1;
      helper = CommonHelper.getHelperInstance();
   }

   /**
    * 
    * @param connectionProperties
    * @param streamOption
    * @return
    */
   public Connection openDBConnection(final Properties connectionProperties,
         final int streamOption) {
      if (connectionProperties.getProperty("database.vendor").equalsIgnoreCase(
            "MySQL")) {

         String url = connectionProperties.getProperty("database.url");
         String dbName = connectionProperties.getProperty("database.name");
         String driver = "com.mysql.jdbc.Driver";
         String userName = connectionProperties
               .getProperty("database.username");
         String password = connectionProperties
               .getProperty("database.password");
         try {
            Class.forName(driver).newInstance();

            // Option returns many more rows in comparison to option 2
            // necessitating the below optimization.
            connect = (Connection) DriverManager.getConnection(url + dbName
                  + "?defaultFetchSize=10000&useCursorFetch=true", userName,
                  password);
            LOGGER.info("Connected to "
                  + connectionProperties.getProperty("database.vendor"));

         } catch (Exception e) {
            LOGGER.error(
                  "Unable to connect to database. Please check the settings", e);
         }

      }
      return connect;
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
      preparedStatement.setFetchSize(Integer.MIN_VALUE);
      try {
         preparedStatement.setTimestamp(1, start);
         preparedStatement.setTimestamp(2, end);
         rs = preparedStatement.executeQuery();
         LOGGER.info("Fetched aggregated records between " + start + " and "
               + end);
      } catch (SQLException e) {
         LOGGER.error("Unable to retreive records", e);

      }
      return rs;
   }

   /**
    * 
    * @param timestamps
    * @param partionByLinkId
    * @return ResultSet
    * @throws SQLException
    */
   public ResultSet retrieveAggregates(final Timestamp[] timestamps,
         boolean partionByLinkId) throws SQLException {
      ResultSet rs = null;
      oddOrEven++;
      synchronized (lock) {
         StringBuffer temp = new StringBuffer("");
         for (int count = 0; count < timestamps.length; count++) {
            if (count == (timestamps.length - 1)) {
               temp.append("?");
            } else {
               temp.append("?,");
            }
         }
         try {
            PreparedStatement preparedStatement = helper.getDBAggregationQuery(
                  partionByLinkId, timestamps, oddOrEven, TABLE_NAME, temp,
                  connect);

            rs = preparedStatement.executeQuery();
            LOGGER.info("Fetched records between " + timestamps[0] + " and "
                  + timestamps[timestamps.length - 1]);

         } catch (SQLException e) {
            LOGGER.error("Unable to retreive records", e);

         }
      }
      return rs;

   }
}
