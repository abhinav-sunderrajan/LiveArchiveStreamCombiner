package mcomp.dissertation.database.streamer.listenersandsubscribers;

import mcomp.dissertation.beans.LiveTrafficBean;

import com.espertech.esper.client.EPRuntime;

public class FilterSubscriber {

   private EPRuntime[] cepRTArray;

   public FilterSubscriber(final EPRuntime[] cepRTArray) {
      this.cepRTArray = cepRTArray;
   }

   /**
    * Send to be joined with archive aggregate after being filtered.
    * @param bean
    */
   public void update(LiveTrafficBean bean) {
      long linkId = bean.getLinkId();
      long bucket = linkId % cepRTArray.length;
      cepRTArray[(int) bucket].sendEvent(bean);

   }

}
