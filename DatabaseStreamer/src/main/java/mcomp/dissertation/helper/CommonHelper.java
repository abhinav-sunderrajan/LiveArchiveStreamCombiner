package mcomp.dissertation.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.vividsolutions.jts.geom.Coordinate;

public class CommonHelper {

   private static CommonHelper helper;
   private static final Logger LOGGER = Logger.getLogger(CommonHelper.class);

   /**
    * 
    * @param streamOption
    * @param dbLoadRate
    * @returns the query for joining the archive and the live data depending
    * upon the mode of operation.
    */

   /**
    * To prevent instantiation
    */
   private CommonHelper() {

   }

   /**
    * 
    * @return singleton instance.
    */
   public static CommonHelper getHelperInstance() {
      if (helper == null) {
         helper = new CommonHelper();
      }
      return helper;
   }

   /**
    * 
    * @param streamOption
    * @param dbLoadRate
    * @returns the join query
    */

   public String getJoinQuery(final int streamOption, long dbLoadRate) {
      String joinQuery;
      if (dbLoadRate < 20) {
         dbLoadRate = 20;
      }
      long reclaimFrequency = 2 * dbLoadRate;
      joinQuery = "@Hint('reclaim_group_aged="
            + dbLoadRate
            + ", reclaim_group_freq="
            + reclaimFrequency
            + "') select live.linkId,live.speed,live.volume,"
            + "historyAgg.linkId, historyAgg.aggregateSpeed,historyAgg.aggregateVolume,live.timeStamp,current_timestamp "
            + "from  mcomp.dissertation.beans.LiveTrafficBean.std:unique(linkId,"
            + "timeStamp.`hours`,timeStamp.`minutes`) as live inner join mcomp.dissertation"
            + ".beans.HistoryAggregateBean.std:unique(linkId,hrs,mins) as historyAgg on historyAgg.linkId"
            + "=live.linkId and historyAgg.mins=live.timeStamp.`minutes` and historyAgg.hrs=live.timeStamp.`hours`";

      return joinQuery;

   }

   public String getAggregationQuery(long dbLoadRate, int streamRate) {
      String aggregationQuery;
      if (dbLoadRate < 50 && dbLoadRate >= 30) {
         dbLoadRate = 50;
      } else if (dbLoadRate < 30) {
         dbLoadRate = 40;
      }
      aggregationQuery = "@Hint('reclaim_group_aged="
            + dbLoadRate
            + ",') select  COUNT(*) as countRec, avg(volume) as avgVolume, avg(speed) as avgSpeed, linkId,readingMinutes as mins, "
            + "readingHours as hrs from mcomp.dissertation.beans.HistoryBean.std:groupwin(linkId,readingMinutes,readingHours)"
            + ".win:time_length_batch(" + (int) (streamRate / 2)
            + " milliseconds, 6) group by linkId,readingMinutes,readingHours";
      return aggregationQuery;

   }

   /**
    * 
    * @param linkIdCoord
    * @param file
    * @param br
    * @throws IOException
    */
   public void loadLinkIdCoordinates(
         ConcurrentHashMap<Long, Coordinate> linkIdCoord, File file,
         BufferedReader br) throws IOException {
      while (br.ready()) {
         String[] items = br.readLine().split(",");
         linkIdCoord.put(
               Long.parseLong(items[0]),
               new Coordinate(Double.parseDouble(items[1]), Double
                     .parseDouble(items[2])));

      }

      br.close();

   }

   /**
    * Return the prepared statement depending on weather the partition by linkID
    * mode of operation is chosen.
    * @return preparedStatement
    * @param partitionByLinkId
    * @param timestamps
    * @param oddOrEven
    * @param linkidRange
    * @param connect
    * @param temp
    * @throws SQLException
    */
   public PreparedStatement getDBAggregationQuery(boolean partitionByLinkId,
         Timestamp[] timestamps, int oddOrEven, String tableName,
         StringBuffer temp, Connection connect) throws SQLException {
      String aggregateQuery = "";
      PreparedStatement preparedStatement = null;
      if (partitionByLinkId) {
         switch (oddOrEven % 2) {
         case 0:
            aggregateQuery = "SELECT LINKID,AVG(SPEED),AVG(VOLUME) FROM "
                  + tableName + " WHERE TIME_STAMP IN(" + temp
                  + ") AND LINKID%2=0 GROUP BY LINKID ORDER BY LINKID";
            LOGGER.info("Even numbered link IDS");
            break;

         case 1:
            aggregateQuery = "SELECT LINKID,AVG(SPEED),AVG(VOLUME) FROM "
                  + tableName + " WHERE TIME_STAMP IN(" + temp
                  + ") AND LINKID%2=1 GROUP BY LINKID ORDER BY LINKID";
            LOGGER.info("Odd numbered link IDS");
            break;

         }
      } else {
         aggregateQuery = "SELECT LINKID,AVG(SPEED),AVG(VOLUME) FROM "
               + tableName + " WHERE TIME_STAMP IN(" + temp
               + ") GROUP BY LINKID ORDER BY LINKID";
      }

      preparedStatement = (PreparedStatement) connect
            .prepareStatement(aggregateQuery);
      preparedStatement.setFetchSize(Integer.MIN_VALUE);
      for (int count = 0; count < timestamps.length; count++) {
         preparedStatement.setTimestamp(count + 1, timestamps[count]);
      }

      return preparedStatement;

   }
}
