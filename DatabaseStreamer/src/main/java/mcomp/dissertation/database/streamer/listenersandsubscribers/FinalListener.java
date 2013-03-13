package mcomp.dissertation.database.streamer.listenersandsubscribers;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import mcomp.dissertation.database.streamer.beans.LiveBean;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/**
 * The listener class for combining the live stream with the aggregate stream.
 */
public class FinalListener implements UpdateListener {

   private DateFormat df;
   private int count = 0;

   public FinalListener() {
      this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
   }

   public void update(final EventBean[] newData, final EventBean[] oldData) {

      Object obj = newData[0].getUnderlying();
      if (obj instanceof HashMap) {
         HashMap<String, Object> msg = (HashMap<String, Object>) obj;
         count++;
         // System.out.println(count + ":"
         // + df.format(Calendar.getInstance().getTime()) + " : " + msg);

      } else {
         LiveBean stockBean = (LiveBean) obj;
      }

   }
}
