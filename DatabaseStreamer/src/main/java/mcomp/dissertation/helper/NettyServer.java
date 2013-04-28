package mcomp.dissertation.helper;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.display.StreamJoinDisplay;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * 
 * This class is responsible for starting an instance of the Netty server to
 * subscribe to live streams sent by the client.
 * 
 */
public class NettyServer<E> {
   private ServerBootstrap bootstrap;
   private Queue<E> buffer;
   private static ChannelFactory factory;
   private ConcurrentHashMap<Long, Coordinate> linkIdCoord;
   private Polygon polygon;
   private GeometryFactory gf;
   private StreamJoinDisplay display;
   private Map<Integer, Double> valueMap;
   private int count;
   private AtomicInteger streamRate;
   private static final Logger LOGGER = Logger.getLogger(NettyServer.class);

   /**
    * The shared buffer to dump the data into.
    * @param buffer
    * @param linkIdCoord
    * @param gf
    * @param polygon
    * @param executor
    * @param streamRate
    */
   @SuppressWarnings("deprecation")
   public NettyServer(final ConcurrentLinkedQueue<E> buffer,
         final ConcurrentHashMap<Long, Coordinate> linkIdCoord,
         final Polygon polygon, final GeometryFactory gf,
         final ScheduledExecutorService executor, final AtomicInteger streamRate) {
      factory = new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool(),
            32);
      bootstrap = new ServerBootstrap(factory);
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
         public ChannelPipeline getPipeline() {
            return Channels.pipeline(
                  new ObjectDecoder(ClassResolvers.cacheDisabled(getClass()
                        .getClassLoader())), new ObjectEncoder(),
                  new FirstHandshake());
         }
      });
      bootstrap.setOption("child.tcpNoDelay", true);
      bootstrap.setOption("child.keepAlive", true);
      this.buffer = buffer;
      this.streamRate = streamRate;
      this.linkIdCoord = linkIdCoord;
      this.polygon = polygon;
      this.gf = gf;
      display = StreamJoinDisplay.getInstance("Join Performance Measure");
      display.addToDataSeries(new TimeSeries(
            "Ingestion rate in messages per second", Minute.class), 3);
      valueMap = new HashMap<Integer, Double>();
      valueMap.put(3, 0.0);
      executor.scheduleAtFixedRate(new IngestionMeasure(), 30, 30,
            TimeUnit.SECONDS);

   }

   /**
    * The server instance listens to the stream represented by <E> on this port.
    * @param port
    */
   public void listen(final int port) {
      bootstrap.bind(new InetSocketAddress(port));
      LOGGER.info("Started server on port " + port);
   }

   private class FirstHandshake extends SimpleChannelHandler {
      @SuppressWarnings("unchecked")
      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
         Channel channel = e.getChannel();
         if (e.getMessage() instanceof String) {
            String msg = (String) e.getMessage();
            if (msg.equalsIgnoreCase("A0092715")) {
               ChannelFuture channelFuture = Channels.future(e.getChannel());
               ChannelEvent responseEvent = new DownstreamMessageEvent(channel,
                     channelFuture, "gandu", channel.getRemoteAddress());
               ctx.sendDownstream(responseEvent);
               super.messageReceived(ctx, e);

            }
         } else {
            E bean = (E) e.getMessage();
            count++;
            // Filter messages outside the defined polygon. To enable scaling to
            // higher data rates.
            if (bean instanceof LiveTrafficBean) {
               LiveTrafficBean live = (LiveTrafficBean) bean;
               long linkId = live.getLinkId();
               Coordinate coord = linkIdCoord.get(linkId);
               Point point = gf.createPoint(coord);
               if (polygon.contains(point)) {
                  buffer.add((E) live);
               }
            }

         }

      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
         e.getCause().printStackTrace();
         e.getChannel().close();
      }
   }

   private class IngestionMeasure implements Runnable {
      int numOfMessages = 0;

      @Override
      public void run() {
         int noOfMsgsin30sec = count - numOfMessages;
         numOfMessages = count;
         if (noOfMsgsin30sec == 0) {
            noOfMsgsin30sec = 1;
            LOGGER.info("No messages received in the past 30 seconds...");
            streamRate.compareAndSet(streamRate.get(), 30000000);
            valueMap.put(3, 0.0);
            display.refreshDisplayValues(valueMap);
         } else {
            streamRate.compareAndSet(streamRate.get(),
                  30000000 / noOfMsgsin30sec);
            LOGGER.info("One message every " + 30000000 / noOfMsgsin30sec
                  + " microsecond");
            valueMap.put(3, noOfMsgsin30sec / 30.0);
            display.refreshDisplayValues(valueMap);

         }

      }
   }

}
