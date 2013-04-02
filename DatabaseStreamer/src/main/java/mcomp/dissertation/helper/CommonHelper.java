package mcomp.dissertation.helper;

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
    * @returns the join query depending upon the mode chosen
    */

   public String getJoinQuery(final int streamOption, long dbLoadRate) {
      String joinQuery;
      if (dbLoadRate < 60) {
         dbLoadRate = 60;
      }

      if (streamOption != 1) {
         joinQuery = "@Hint('reclaim_group_aged="
               + dbLoadRate
               + ", reclaim_group_freq=30') select * from  mcomp.dissertation.database.streamer.beans.LiveBean.std:unique(linkId,"
               + "timeStamp.`hours`,timeStamp.`minutes`) as live inner join mcomp.dissertation.database.streamer"
               + ".beans.HistoryAggregateBean.std:unique(linkId,hrs,mins) as historyAgg on historyAgg.linkId"
               + "=live.linkId and historyAgg.mins=live.timeStamp.`minutes` and historyAgg.hrs=live.timeStamp.`hours`";

      } else {
         joinQuery = "@Hint('reclaim_group_aged="
               + dbLoadRate
               + ",reclaim_group_freq=30') select * from  mcomp.dissertation.database.streamer.beans.LiveBean as live unidirectional "
               + " inner join mcomp.dissertation.database.streamer.beans.HistoryAggregateBean.std:unique(linkId,hrs,mins) as historyAgg"
               + "  on historyAgg.linkId=live.linkId and historyAgg.mins=live.timeStamp.`minutes` and historyAgg.hrs=live.timeStamp.`hours`";
      }
      return joinQuery;

   }
}
