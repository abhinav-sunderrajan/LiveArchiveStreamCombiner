package mcomp.dissertation.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.vividsolutions.jts.geom.Coordinate;

public class CommonHelper {

   private static CommonHelper helper;

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
}
