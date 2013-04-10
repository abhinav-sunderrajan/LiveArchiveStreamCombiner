package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * 
 * This class is responsible for changing the rate at which data is loaded from
 * the DB and streamed of the buffer to which it's loaded.
 */
public class StreamRateChanger implements Runnable {

   private AtomicInteger streamRate;
   private GenericArchiveStreamer<?>[] streamers;
   private AbstractLoader<?>[] histroyLoaders;
   private long dbLoadRate;
   private float streamRateSpeedUp;
   private ScheduledFuture<?>[] dbLoadFutures;
   private ScheduledFuture<?>[] archiveStreamFutures;
   private ScheduledExecutorService executor;
   private Object monitor;
   private boolean wakeFlag;
   private static final Logger LOGGER = Logger
         .getLogger(StreamRateChanger.class);

   /**
    * Parameters required to change the rate at which data is loaded from the DB
    * and streamed of the buffer to which it's loaded.
    * @param streamRate
    * @param archiveStreamers
    * @param histroyLoader
    * @param archiveStreamFutures
    * @param dbLoadFuture
    * @param configProperties
    * @param executor
    * @param monitor
    */
   public StreamRateChanger(final AtomicInteger streamRate,
         final GenericArchiveStreamer<?>[] archiveStreamers,
         final AbstractLoader<?>[] histroyLoader,
         final ScheduledFuture<?>[] archiveStreamFutures,
         final ScheduledFuture<?>[] dbLoadFuture,
         final Properties configProperties,
         final ScheduledExecutorService executor, final Object monitor) {
      this.streamRate = streamRate;
      this.streamers = archiveStreamers;
      this.histroyLoaders = histroyLoader;
      wakeFlag = true;
      this.executor = executor;
      dbLoadRate = (long) (streamRate.get() * Float.parseFloat(configProperties
            .getProperty("db.prefetch.rate")));
      streamRateSpeedUp = Float.parseFloat(configProperties
            .getProperty("archive.stream.rate.param"));
      this.monitor = monitor;

   }

   public void run() {

      if (wakeFlag) {
         synchronized (monitor) {
            LOGGER.info("Wait for stream arrival before sampling the stream rate");
            try {
               monitor.wait();
            } catch (InterruptedException e) {
               LOGGER.error("Error waiting on monitor", e);
            }
         }
      }
      wakeFlag = false;

      executor.schedule(new Runnable() {
         public void run() {
            for (int count = 0; count < histroyLoaders.length; count++) {
               dbLoadFutures[count].cancel(true);
               dbLoadFutures[count] = executor.scheduleAtFixedRate(
                     histroyLoaders[count], 0, dbLoadRate, TimeUnit.SECONDS);
            }
         }
      }, 1, TimeUnit.SECONDS);

      executor.schedule(new Runnable() {
         public void run() {
            for (int count = 0; count < streamers.length; count++) {
               archiveStreamFutures[count].cancel(true);
               archiveStreamFutures[count] = executor.scheduleWithFixedDelay(
                     streamers[count], 0,
                     (long) (streamRate.get() * streamRateSpeedUp),
                     TimeUnit.MICROSECONDS);
            }
         }
      }, 1, TimeUnit.SECONDS);

      LOGGER.info("Changed DB load rate to " + dbLoadRate
            + " seconds and archive streamer rates to " + streamRate
            + " microseconds to match live stream rate.");
   }
}
