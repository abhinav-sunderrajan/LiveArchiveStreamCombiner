package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.database.streamer.beans.HistoryBean;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

/**
 * This thread is responsible removing records from the buffer as a streamer and
 * sending it to Esper be aggregated with other archive streams.
 */
public class RecordStreamer {
   private static final Logger LOGGER = Logger.getLogger(RecordStreamer.class);
   private Runnable runnable;
   private ScheduledExecutorService executor;
   private AtomicInteger streamRate;
   private static SimpleDateFormat dfLocal = new SimpleDateFormat(
         "dd-MMM-yyyy HH:mm:ss.SSS");

   /**
    * @param buffer
    * @param cepRTAggregateArray
    */
   public RecordStreamer(final ConcurrentLinkedQueue<HistoryBean> buffer,
         final EPRuntime[] cepRTAggregateArray, final Object monitor,
         final ScheduledExecutorService executor, final AtomicInteger streamRate) {
      this.executor = executor;
      this.streamRate = streamRate;
      this.runnable = new Runnable() {

         private int count = 0;

         public void run() {

            // Release the lock on the monitor lock to release all waiting
            // threads. Applicable only for the first time.
            if (count == 0) {
               synchronized (monitor) {
                  LOGGER.info("Wait for the initial data base load before streaming..");
                  try {
                     monitor.wait();
                     LOGGER.info("Awake!! Starting to streaming now");
                  } catch (InterruptedException e) {
                     e.printStackTrace();
                  }
               }
            }
            while (buffer.isEmpty()) {
               // Poll till the producer has filled the queue. Bad approach will
               // optimize this.
            }

            HistoryBean history = buffer.poll();
            long bucket = history.getLinkId() % cepRTAggregateArray.length;
            cepRTAggregateArray[(int) bucket].sendEvent(history);
            count++;
            // // print for evaluation purposes only..
            // if (count % 1000 == 0) {
            // System.out.println(count + " " + streamRate.get() + " "
            // + dfLocal.format(Calendar.getInstance().getTime()));
            // }

         }
      };

   }

   public ScheduledFuture<?> startStreaming() {
      ScheduledFuture<?> archiveFuture = null;
      archiveFuture = executor.scheduleAtFixedRate(runnable, 0,
            streamRate.get(), TimeUnit.MICROSECONDS);
      return archiveFuture;

   }

   /**
    * @return the runnable
    */
   public Runnable getRunnable() {
      return runnable;
   }

}
