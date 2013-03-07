package mcomp.dissertation.databse.streamer.listeners;

import java.util.HashMap;

import mcomp.dissertation.database.streamer.beans.HistoryAggregateBean;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/**
 * The listener for aggregating all historical sub-streams.
 */
public class AggregateListener implements UpdateListener {

   private EPRuntime cepRT;

   /**
    * @param cepRT -- Use this event processing run time service to send the
    * aggregated data to be joined with the live stream.
    */
   public AggregateListener(final EPRuntime cepRT) {
      this.cepRT = cepRT;

   }

   public void update(final EventBean[] newData, final EventBean[] oldData) {

      Object obj = newData[0].getUnderlying();
      if (obj instanceof HashMap) {
         HashMap<String, Object> msg = (HashMap<String, Object>) obj;
         if ((Long) msg.get("countRec") > 0) {
            // System.out.println(msg);
            HistoryAggregateBean aggBean = new HistoryAggregateBean();
            aggBean.setAggregateSpeed((Double) msg.get("avgSpeed"));
            aggBean.setAggregateVolume((Double) msg.get("avgVolume"));
            aggBean.setLinkId((Long) msg.get("linkId"));
            aggBean.setMins((Integer) msg.get("mins"));
            aggBean.setHrs((Integer) msg.get("hrs"));
            cepRT.sendEvent(aggBean);
         }
      }
   }

}
