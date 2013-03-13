package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamRateChanger implements Runnable {

   private AtomicInteger streamRate;
   private Random random;
   private RecordStreamer[] streamers;
   private ScheduledFuture<?>[] futures;
   private ScheduledExecutorService executor;
   private ScheduledFuture<?> liveFuture;
   private Runnable liveRunnable;

   public StreamRateChanger(final AtomicInteger streamRate,
         final RecordStreamer[] streamers, final ScheduledFuture<?>[] futures,
         final ScheduledExecutorService executor,
         final ScheduledFuture<?> liveFuture, final Runnable liveRunnable) {
      this.streamRate = streamRate;
      random = new Random();
      this.futures = futures;
      this.streamers = streamers;
      this.executor = executor;
      this.liveFuture = liveFuture;
      this.liveRunnable = liveRunnable;
   }

   public void run() {
      int val = random.nextInt(50000);

      val = (val < 10000) ? 10000 : val;
      streamRate.compareAndSet(streamRate.get(), val);

      // Change rate at which scheduled executors streams archive data and the
      // liveData.
      liveFuture.cancel(true);
      liveFuture = executor.scheduleWithFixedDelay(liveRunnable, 0,
            streamRate.get(), TimeUnit.MICROSECONDS);

      for (int count = 0; count < streamers.length; count++) {
         futures[count].cancel(true);
         futures[count] = executor.scheduleWithFixedDelay(
               streamers[count].getRunnable(), 0, streamRate.get() / 2,
               TimeUnit.MICROSECONDS);

      }
   }
}
