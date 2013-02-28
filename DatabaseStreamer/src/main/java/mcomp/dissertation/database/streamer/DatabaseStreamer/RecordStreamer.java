package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.database.streamer.beans.HistoryBean;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

/**
 * This thread is responsible removing records from the buffer as a streamer and
 * sending it to Esper be aggregated with other archive streams.
 * 
 */
public class RecordStreamer extends Thread {
   private static final Logger LOGGER = Logger.getLogger(RecordStreamer.class);
   private ConcurrentLinkedQueue<HistoryBean> buffer;
   private int streamRate;
   private EPRuntime cepRTAggegate;
   private Object monitor;

   /**
    * @param buffer
    * @param streamRate
    * @param cepRTAggegate
    * @param monitor
    */
   public RecordStreamer(ConcurrentLinkedQueue<HistoryBean> buffer,
         int streamRate, EPRuntime cepRTAggegate, Object monitor) {
      this.buffer = buffer;
      this.streamRate = streamRate;
      this.cepRTAggegate = cepRTAggegate;
      this.monitor = monitor;

   }

   @Override
   public void run() {
      while (buffer.isEmpty()) {
         // Poll till the producer has filled the queue. bad approach will
         // optimize this.

      }

      // wait for the live streamer to start before starting the archive
      // streams.
      // try {
      // synchronized (monitor) {
      // monitor.wait();
      // }
      //
      // } catch (InterruptedException e) {
      // LOGGER.error("Interrupted while waiting on lock", e);
      // }
      while (true) {
         try {
            HistoryBean history = buffer.poll();
            cepRTAggegate.sendEvent(history);
            Thread.sleep(streamRate);
         } catch (InterruptedException e) {
            LOGGER.error("Archive stream failed", e);
         }

      }
   }
}
