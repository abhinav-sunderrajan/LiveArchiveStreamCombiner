package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.database.streamer.beans.HistoryAggregateBean;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

public class AggregateStreamer {
   private static final Logger LOGGER = Logger.getLogger(RecordStreamer.class);
   private Runnable runnable;
   private ScheduledExecutorService executor;
   private AtomicInteger streamRate;
   private static SimpleDateFormat dfLocal = new SimpleDateFormat(
         "dd-MMM-yyyy HH:mm:ss.SSS");
   private float streamRateSpeedUp;

   /**
    * 
    * @param buffer
    * @param cepRTJoinArray
    * @param monitor
    * @param executor
    * @param streamRate
    * @param streamRateParam
    */
   public AggregateStreamer(
         final ConcurrentLinkedQueue<HistoryAggregateBean> buffer,
         final EPRuntime[] cepRTJoinArray, final Object monitor,
         final ScheduledExecutorService executor,
         final AtomicInteger streamRate, final float streamRateParam) {
      this.executor = executor;
      this.streamRate = streamRate;
      this.streamRateSpeedUp = streamRateParam;
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

            HistoryAggregateBean history = buffer.poll();
            long bucket = history.getLinkId() % cepRTJoinArray.length;
            cepRTJoinArray[(int) bucket].sendEvent(history);
            count++;
            // print for evaluation purposes only..
            // if (count % 1000 == 0) {
            // System.out
            // .println(count + " "
            // + dfLocal.format(Calendar.getInstance().getTime())
            // + " " + history.getLinkId() + "<-->"
            // + history.getAggregateSpeed() + " at "
            // + history.getMins());
            // }

         }
      };

   }

   public ScheduledFuture<?> startStreaming() {

      // Drive the archive stream a bit faster than the live to compensate for
      // the time required for aggregation.
      ScheduledFuture<?> archiveFuture = executor.scheduleAtFixedRate(runnable,
            0, (long) (streamRate.get() * streamRateSpeedUp),
            TimeUnit.MICROSECONDS);
      return archiveFuture;

   }

   /**
    * @return the runnable
    */
   public Runnable getRunnable() {
      return runnable;
   }

}
