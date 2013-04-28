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
   private float dbPrefetchParam;
   private float streamRateSpeedUp;
   private ScheduledFuture<?>[] dbLoadFutures;
   private ScheduledFuture<?>[] archiveStreamFutures;
   private ScheduledExecutorService executor;
   private Object monitor;
   private boolean wakeFlag;
   private long dbLoadRate;
   private int streamRatePrev;
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
         final ScheduledFuture<?>[] dbLoadFutures,
         final Properties configProperties,
         final ScheduledExecutorService executor, final Object monitor) {
      this.streamRate = streamRate;
      streamRatePrev = streamRate.get();
      this.streamers = archiveStreamers;
      this.histroyLoaders = histroyLoader;
      wakeFlag = true;
      this.executor = executor;
      this.archiveStreamFutures = archiveStreamFutures;
      this.dbLoadFutures = dbLoadFutures;
      dbPrefetchParam = Float.parseFloat(configProperties
            .getProperty("db.prefetch.rate"));
      dbLoadRate = (long) (streamRate.get() * dbPrefetchParam);
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

      // Change archive stream rate
      int percentChange = (Math.abs(streamRatePrev - streamRate.get()) * 100 / streamRatePrev);
      if (percentChange > 20) {
         LOGGER.info("Significant change in the live stream rate.. exceeding 20% hence changing");
         executor.schedule(new Runnable() {
            public void run() {
               for (int count = 0; count < streamers.length; count++) {
                  archiveStreamFutures[count].cancel(true);
                  archiveStreamFutures[count] = executor
                        .scheduleWithFixedDelay(streamers[count], 0,
                              (long) (streamRateSpeedUp * streamRate.get()),
                              TimeUnit.MICROSECONDS);
                  streamRatePrev = streamRate.get();
               }
            }

         }, 1, TimeUnit.SECONDS);
         LOGGER.info("Retaining DB load rate at " + dbLoadRate
               + " seconds but changing archive streamer rates to "
               + streamRate.get() + " microseconds to match live stream rate.");
      } else {
         streamRatePrev = streamRate.get();
         LOGGER.info("Not changing archive stream rate the change is insignificant and equal to "
               + percentChange + "%");
      }
      // Change DB pre-fetch rate.
      // executor.schedule(new Runnable() {
      // public void run() {
      // for (int count = 0; count < histroyLoaders.length; count++) {
      // dbLoadFutures[count].cancel(true);
      // dbLoadRate = (long) (streamRate.get() * dbPrefetchParam);
      // dbLoadFutures[count] = executor.scheduleAtFixedRate(
      // histroyLoaders[count], 0, dbLoadRate, TimeUnit.SECONDS);
      // }
      // }
      // }, 1, TimeUnit.SECONDS);

   }
}
