package mcomp.dissertation.helper;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

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
   private static final Logger LOGGER = Logger.getLogger(NettyServer.class);

   /**
    * The shared buffer to dump the data into.
    * @param buffer
    */
   public NettyServer(final ConcurrentLinkedQueue<E> buffer) {
      factory = new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
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
            buffer.add(bean);
         }

      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
         e.getCause().printStackTrace();
         e.getChannel().close();
      }
   }

}
