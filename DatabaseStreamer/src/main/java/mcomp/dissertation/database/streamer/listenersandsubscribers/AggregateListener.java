package mcomp.dissertation.database.streamer.listenersandsubscribers;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import mcomp.dissertation.beans.HistoryAggregateBean;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/**
 * The listener for aggregating all historical sub-streams.
 */
public class AggregateListener implements UpdateListener {

   private EPRuntime cepRT;
   private int count = 0;
   private DateFormat df;

   /**
    * @param cepRT -- Use this event processing run time service to send the
    * aggregated data to be joined with the live stream.
    */
   public AggregateListener(final EPRuntime cepRT) {
      this.cepRT = cepRT;
      this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");

   }

   public void update(final EventBean[] newData, final EventBean[] oldData) {

      Object obj = newData[0].getUnderlying();
      if (obj instanceof HashMap) {
         HashMap<String, Object> msg = (HashMap<String, Object>) obj;
         if ((Long) msg.get("countRec") > 0) {
            HistoryAggregateBean aggBean = new HistoryAggregateBean();
            aggBean.setAggregateSpeed((Double) msg.get("avgSpeed"));
            aggBean.setAggregateVolume((Double) msg.get("avgVolume"));
            aggBean.setLinkId((Long) msg.get("linkId"));
            aggBean.setMins((Integer) msg.get("mins"));
            aggBean.setHrs((Integer) msg.get("hrs"));
            cepRT.sendEvent(aggBean);
            count++;
            // print for evaluation purposes only..
            if (count % 1000 == 0) {
               System.out.println(count + " : " + this.hashCode() + " : "
                     + df.format(Calendar.getInstance().getTime()) + ":" + msg);
            }

         }
      }
   }
}
