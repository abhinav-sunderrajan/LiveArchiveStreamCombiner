package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.database.streamer.beans.HistoryBean;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

/**
 * This thread is responsible removing records from the buffer as a streamer and
 * sending it to Esper be aggregated with other archive streams.
 */
public class RecordStreamer implements Runnable {
   private static final Logger LOGGER = Logger.getLogger(RecordStreamer.class);
   private ConcurrentLinkedQueue<HistoryBean> buffer;
   private EPRuntime cepRTAggegate;

   /**
    * @param buffer
    * @param cepRTAggegate
    */
   public RecordStreamer(final ConcurrentLinkedQueue<HistoryBean> buffer,
         final EPRuntime cepRTAggegate) {
      this.buffer = buffer;
      this.cepRTAggegate = cepRTAggegate;
   }

   public void run() {

      while (buffer.isEmpty()) {
         // Poll till the producer has filled the queue. bad approach will
         // optimize this.
      }

      HistoryBean history = buffer.poll();
      cepRTAggegate.sendEvent(history);

   }
}
