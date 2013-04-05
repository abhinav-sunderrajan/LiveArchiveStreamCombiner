package mcomp.dissertation.database.streamer.listenersandsubscribers;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import mcomp.dissertation.beans.HistoryAggregateBean;

import com.espertech.esper.client.EPRuntime;

/**
 * 
 * @author This is a subscriber listening to the historic aggregate stream.
 * Advantage of being quicker an less cumbersome than a listener.
 * 
 */

public class AggregateSubscriber {

   private EPRuntime[] cepRTJoinArray;
   private DateFormat df;
   private int count;

   /**
    * @param cepRT -- Use this event processing run time service to send the
    * aggregated data to be joined with the live stream.
    */
   public AggregateSubscriber(final EPRuntime[] cepRT) {
      this.cepRTJoinArray = cepRT;
      this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
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
         // if (count % 1000 == 0) {
         // System.out.println(count + " The aggregator hashCode: "
         // + this.hashCode() + " : "
         // + df.format(Calendar.getInstance().getTime())
         // + " Number of records :" + countRec.longValue() + " Bean: "
         // + aggBean);
         // }
      }

   }

}
