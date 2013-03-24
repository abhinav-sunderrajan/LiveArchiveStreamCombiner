package mcomp.dissertation.database.streamer.listenersandsubscribers;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import mcomp.dissertation.database.streamer.beans.HistoryAggregateBean;
import mcomp.dissertation.database.streamer.beans.LiveBean;

import org.apache.log4j.Logger;

public class FinalSubscriber {

   private DateFormat df;
   private int count = 0;
   private static final Logger LOGGER = Logger.getLogger(FinalSubscriber.class);

   public FinalSubscriber() {
      this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
   }

   public void update(Long liveLinkId, Float liveSpeed, Float liveVolume,
         Timestamp liveTimeStamp, String liveEventTime, Long historyLinkid,
         Double historyAvgSpeed, Double historyAvgVolume) {
      count++;
      if (count % 1000 == 0) {
         LOGGER.info("hashCode:" + this.hashCode() + " " + count + ":"
               + df.format(Calendar.getInstance().getTime())
               + " [Details(EventTime:" + liveEventTime
               + " Linkid live and history(" + liveLinkId + "--"
               + historyLinkid + "), speeds live and history(" + liveSpeed
               + "--" + historyAvgSpeed + "), volume live and history("
               + liveVolume + "--" + historyAvgVolume + ") Live time stamp("
               + liveTimeStamp + ")]");
      }

   }

   public void update(LiveBean live, HistoryAggregateBean history) {
      count++;
      if (count % 1000 == 0) {
         if (history != null) {
            LOGGER.info("hashCode:" + this.hashCode() + " " + count + ":"
                  + df.format(Calendar.getInstance().getTime())
                  + " [Details(EventTime:" + live.getEventTime()
                  + " Linkid live and history(" + live.getLinkId() + "--"
                  + history.getLinkId() + "), speeds live and history("
                  + live.getAvgSpeed() + "--" + history.getAggregateSpeed()
                  + "), volume live and history(" + live.getAvgVolume() + "--"
                  + history.getAggregateVolume() + ") Live time stamp("
                  + live.getTimeStamp() + ")]");
         } else {

            LOGGER.info("hashCode:" + this.hashCode() + " " + count + ":"
                  + df.format(Calendar.getInstance().getTime())
                  + " [Details(EventTime:" + live.getEventTime()
                  + " Linkid live and history(" + live.getLinkId()
                  + "-- null ), speeds live and history(" + live.getAvgSpeed()
                  + "-- null), volume live and history(" + live.getAvgVolume()
                  + "-- null) Live time stamp(" + live.getTimeStamp() + ")]");

         }
      }

   }

}
