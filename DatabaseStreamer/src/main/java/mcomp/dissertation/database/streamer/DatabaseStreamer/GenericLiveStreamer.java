package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.text.DateFormat;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.helper.NettyServer;

import com.espertech.esper.client.EPRuntime;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

public class GenericLiveStreamer<E> implements Runnable {
   private ScheduledExecutorService executor;
   private Object monitor;
   private Queue<E> buffer;
   private EPRuntime[] cepRTArray;
   private int port;
   private AtomicInteger streamRate;

   /**
    * 
    * @param buffer
    * @param cepRTArray
    * @param monitor
    * @param executor
    * @param streamRate
    * @param df
    * @param linkIdCoord
    * @param polygon
    * @param gf
    */
   public GenericLiveStreamer(final ConcurrentLinkedQueue<E> buffer,
         final EPRuntime[] cepRTArray, final Object monitor,
         final ScheduledExecutorService executor,
         final AtomicInteger streamRate, final DateFormat df, final int port,
         final GeometryFactory gf, final Polygon polygon,
         final ConcurrentHashMap<Long, Coordinate> linkIdCoord) {
      this.buffer = buffer;
      this.cepRTArray = cepRTArray;
      this.monitor = monitor;
      this.executor = executor;
      this.streamRate = streamRate;
      this.port = port;
      new AtomicLong(0);
      startListening(linkIdCoord, polygon, gf, executor, streamRate);

   }

   private void startListening(ConcurrentHashMap<Long, Coordinate> linkIdCoord,
         Polygon polygon, GeometryFactory gf,
         ScheduledExecutorService executor, AtomicInteger streamRate) {
      NettyServer<E> server = new NettyServer<E>(
            (ConcurrentLinkedQueue<E>) buffer, linkIdCoord, polygon, gf,
            executor, streamRate);
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
         cepRTArray[(int) bucket].sendEvent(bean);

      }

   }

   public ScheduledFuture<?> startStreaming() {

      // Drive the live stream at the given rate specified.
      ScheduledFuture<?> liveFuture = executor.scheduleAtFixedRate(this, 0,
            (long) (streamRate.get()), TimeUnit.MICROSECONDS);
      return liveFuture;

   }

}
