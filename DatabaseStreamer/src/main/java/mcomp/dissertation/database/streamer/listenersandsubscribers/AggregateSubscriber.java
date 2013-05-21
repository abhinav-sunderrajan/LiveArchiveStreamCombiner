package mcomp.dissertation.database.streamer.listenersandsubscribers;

import java.text.SimpleDateFormat;

import mcomp.dissertation.beans.HistoryAggregateBean;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

/**
 * 
 * @author This is a subscriber listening to the historic aggregate stream.
 * Advantage of being quicker an less cumbersome than a listener.
 * 
 */

public class AggregateSubscriber {

   private EPRuntime[] cepRTJoinArray;
   private int count;
   private static final Logger LOGGER = Logger
         .getLogger(AggregateSubscriber.class);

   /**
    * @param cepRT -- Use this event processing run time service to send the
    * aggregated data to be joined with the live stream.
    */
   public AggregateSubscriber(final EPRuntime[] cepRT) {
      this.cepRTJoinArray = cepRT;
      new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
      count = 0;

   }

   /**
    * The method called by the Esper engine instance on aggregating the historic
    * data stream.
    * @param countRec
    * @param avgVolume
    * @param avgSpeed
    * @param linkId
    * @param mins
    * @param hrs
    */

   public void update(Long countRec, Double avgVolume, Double avgSpeed,
         long linkId, Integer mins, Integer hrs) {

      if (!(avgVolume == null || avgSpeed == null)) {
         HistoryAggregateBean aggBean = new HistoryAggregateBean();
         aggBean.setAggregateSpeed(avgSpeed);
         aggBean.setAggregateVolume(avgVolume);
         aggBean.setLinkId(linkId);
         aggBean.setMins(mins);
         aggBean.setHrs(hrs);
         long bucket = linkId % cepRTJoinArray.length;
         cepRTJoinArray[(int) bucket].sendEvent(aggBean);
         count++;
         // print for evaluation purposes only..
         if (count % 5000 == 0) {
            LOGGER.info(" Number of records :" + countRec.longValue()
                  + " link: " + linkId + " speed " + avgSpeed + " at " + hrs
                  + ":" + mins);
         }
      }

   }
}
