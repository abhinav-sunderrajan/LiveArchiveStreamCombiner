package mcomp.dissertation.databse.streamer.listeners;

import java.util.HashMap;

import mcomp.dissertation.database.streamer.beans.LiveBean;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/**
 * The listener class for combining the live stream with the aggregate stream.
 * 
 */
public class FinalListener implements UpdateListener {

   public void update(EventBean[] newData, EventBean[] oldData) {

      Object obj = newData[0].getUnderlying();
      if (obj instanceof HashMap) {
         HashMap<String, Object> msg = (HashMap<String, Object>) obj;
         System.out.println("here" + msg);

      } else {
         LiveBean stockBean = (LiveBean) obj;
      }

   }
}
