package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.database.streamer.RDBMSAccess.DBConnect;

public abstract class AbstractLoader<T> implements Runnable {
   private Queue<T> buffer;
   private DBConnect dbconnect;
   protected Object monitor;
   protected boolean wakeFlag;
   protected static final long REFRESH_INTERVAL = 300000;

   /**
    * 
    * @param buffer
    * @param connectionProperties
    * @param monitor
    */
   public AbstractLoader(final ConcurrentLinkedQueue<T> buffer,
         final Properties connectionProperties, final Object monitor) {
      this.buffer = buffer;
      dbconnect = new DBConnect();
      dbconnect.openDBConnection(connectionProperties);
      this.monitor = monitor;
      this.wakeFlag = true;
   }

   public Queue<T> getBuffer() {
      return buffer;
   }

   /**
    * 
    * @return the established database connection
    */
   public DBConnect getDBConnection() {
      return dbconnect;
   }

}
