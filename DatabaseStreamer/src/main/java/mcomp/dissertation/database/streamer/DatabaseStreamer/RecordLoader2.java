package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.database.streamer.RDBMSAccess.DBConnect;
import mcomp.dissertation.database.streamer.beans.HistoryAggregateBean;

import org.apache.log4j.Logger;

public class RecordLoader2 implements Runnable {

   private Queue<HistoryAggregateBean> buffer;
   private Timestamp[] startTimes;
   private DBConnect dbconnect;
   private Object monitor;
   private boolean wakeFlag;
   private static final Logger LOGGER = Logger.getLogger(RecordLoader.class);
   private static final long REFRESH_INTERVAL = 300000;

   /**
    * 
    * @param buffer
    * @param startTime
    * @param connectionProperties
    * @param monitor
    */
   public RecordLoader2(
         final ConcurrentLinkedQueue<HistoryAggregateBean> buffer,
         final Timestamp[] startTimes, final Properties connectionProperties,
         final Object monitor) {
      this.buffer = buffer;
      this.startTimes = startTimes;
      dbconnect = new DBConnect();
      dbconnect.openDBConnection(connectionProperties);
      this.monitor = monitor;
      this.wakeFlag = true;

   }

   @SuppressWarnings("deprecation")
   public void run() {
      try {
         ResultSet rs = dbconnect.retrieveAggregates(startTimes);

         // The stream threads need to woken up only for the first database
         // load..
         if (wakeFlag) {
            synchronized (monitor) {
               LOGGER.info("Waking the streamer threads..");
               monitor.notifyAll();
            }
         }

         while (rs.next()) {
            HistoryAggregateBean bean = new HistoryAggregateBean();
            bean.setLinkId(rs.getInt(1));
            bean.setAggregateSpeed(rs.getFloat(2));
            bean.setAggregateVolume(rs.getInt(3));
            bean.setMins(startTimes[0].getMinutes());
            bean.setHrs(startTimes[0].getHours());
            buffer.add(bean);
         }

         // Update the time stamps for the next fetch.
         for (int count = 0; count < startTimes.length; count++) {
            long start = startTimes[count].getTime() + REFRESH_INTERVAL;
            startTimes[count] = new Timestamp(start);
         }
         wakeFlag = false;
      } catch (SQLException e) {
         LOGGER.error("Error accessing the database to retrieve archived data",
               e);
      }

   }
}
