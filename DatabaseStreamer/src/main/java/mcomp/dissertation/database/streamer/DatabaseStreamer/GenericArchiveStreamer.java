package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.database.streamer.beans.HistoryAggregateBean;
import mcomp.dissertation.database.streamer.beans.HistoryBean;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

public class GenericArchiveStreamer<T> implements Runnable {

   private ScheduledExecutorService executor;
   private AtomicInteger streamRate;
   private float streamRateSpeedUp;
   private int count;
   private Object monitor;
   private static final Logger LOGGER = Logger
         .getLogger(GenericArchiveStreamer.class);
   private Queue<T> buffer;
   private EPRuntime[] cepRTArray;
   private DateFormat dfLocal;

   /**
    * 
    * @param buffer
    * @param cepRTJoinArray
    * @param monitor
    * @param executor
    * @param streamRate
    */
   public GenericArchiveStreamer(final ConcurrentLinkedQueue<T> buffer,
         final EPRuntime[] cepRTArray, final Object monitor,
         final ScheduledExecutorService executor,
         final AtomicInteger streamRate, final float streamRateSpeedUp) {
      this.buffer = buffer;
      this.cepRTArray = cepRTArray;
      this.monitor = monitor;
      this.executor = executor;
      this.streamRate = streamRate;
      this.streamRateSpeedUp = streamRateSpeedUp;
      this.dfLocal = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");

   }

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

      T obj = buffer.poll();
      if (obj instanceof HistoryBean) {
         long bucket = ((HistoryBean) obj).getLinkId() % cepRTArray.length;
         cepRTArray[(int) bucket].sendEvent(obj);
         count++;

      }

      if (obj instanceof HistoryAggregateBean) {
         long bucket = ((HistoryAggregateBean) obj).getLinkId()
               % cepRTArray.length;
         cepRTArray[(int) bucket].sendEvent(obj);
         count++;

      }

      // print for evaluation purposes only..
      // if (count % 1000 == 0) {
      // System.out.println(count + " "
      // + dfLocal.format(Calendar.getInstance().getTime()));
      // }

   }

   public ScheduledFuture<?> startStreaming() {

      // Drive the archive stream a bit faster than the live to compensate for
      // the time required for aggregation.
      ScheduledFuture<?> archiveFuture = executor.scheduleAtFixedRate(this, 0,
            (long) (streamRate.get() * streamRateSpeedUp),
            TimeUnit.MICROSECONDS);
      return archiveFuture;

   }

}
