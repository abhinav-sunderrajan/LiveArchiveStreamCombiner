package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.text.DateFormat;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.beans.LiveWeatherBean;
import mcomp.dissertation.helper.NettyServer;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

public class GenericLiveStreamer<E> implements Runnable {
   private ScheduledExecutorService executor;
   private int count;
   private Object monitor;
   private Queue<E> buffer;
   private EPRuntime[] cepRTArray;
   private AtomicInteger streamRate;
   private int port;
   private static final Logger LOGGER = Logger
         .getLogger(GenericLiveStreamer.class);

   /**
    * 
    * @param buffer
    * @param cepRTArray
    * @param monitor
    * @param executor
    * @param streamRate
    * @param df
    */
   public GenericLiveStreamer(final ConcurrentLinkedQueue<E> buffer,
         final EPRuntime[] cepRTArray, final Object monitor,
         final ScheduledExecutorService executor,
         final AtomicInteger streamRate, final DateFormat df, final int port) {
      this.buffer = buffer;
      this.cepRTArray = cepRTArray;
      this.monitor = monitor;
      this.executor = executor;
      this.streamRate = streamRate;
      this.port = port;
      startListening();

   }

   private void startListening() {
      NettyServer<E> server = new NettyServer<E>(
            (ConcurrentLinkedQueue<E>) buffer);
      server.listen(port);
   }

   @Override
   public void run() {
      while (buffer.isEmpty()) {
         // Poll till the producer has filled the queue. Bad approach will
         // optimize this.
      }
      synchronized (monitor) {
         monitor.notifyAll();
      }
      E obj = buffer.poll();
      if (obj instanceof LiveTrafficBean) {
         LiveTrafficBean bean = (LiveTrafficBean) obj;
         long linkId = bean.getLinkId();
         long bucket = linkId % cepRTArray.length;
         cepRTArray[(int) bucket].sendEvent(obj);
         count++;
      }

      if (obj instanceof LiveWeatherBean) {
         LiveWeatherBean bean = (LiveWeatherBean) obj;
         long linkId = bean.getLinkId();
         long bucket = linkId % cepRTArray.length;
         cepRTArray[(int) bucket].sendEvent(obj);
         count++;
      }

   }

   public ScheduledFuture<?> startStreaming() {

      // Drive the live stream at the given rate specified.
      ScheduledFuture<?> liveFuture = executor.scheduleAtFixedRate(this, 0,
            (long) (streamRate.get()), TimeUnit.MICROSECONDS);
      return liveFuture;

   }

}
