package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.database.streamer.RDBMSAccess.DBConnect;

import org.apache.log4j.Logger;

import com.mysql.jdbc.Connection;

public abstract class AbstractLoader<T> implements Runnable {
   private Queue<T> buffer;
   protected DBConnect dbconnect;
   protected Object monitor;
   protected boolean wakeFlag;
   protected static final long REFRESH_INTERVAL = 300000;
   private Connection connect;
   private static final Logger LOGGER = Logger.getLogger(AbstractLoader.class);
   private Object lock;

   /**
    * 
    * @param buffer
    * @param connectionProperties
    * @param monitor
    */
   public AbstractLoader(final ConcurrentLinkedQueue<T> buffer,
         final Properties connectionProperties, final Object monitor,
         final int streamOption) {
      this.buffer = buffer;
      lock = new Object();
      dbconnect = new DBConnect(lock);
      connect = dbconnect.openDBConnection(connectionProperties, streamOption);
      this.monitor = monitor;
      this.wakeFlag = true;

   }

   public Queue<T> getBuffer() {
      return buffer;
   }

}
