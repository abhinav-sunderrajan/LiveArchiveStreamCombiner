package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.database.streamer.beans.HistoryAggregateBean;

import org.apache.log4j.Logger;

public class RecordLoaderAggregate<T> extends AbstractLoader<T> {

   private Timestamp[] startTimes;
   private static final Logger LOGGER = Logger.getLogger(RecordLoader.class);

   /**
    * 
    * @param buffer
    * @param startTime
    * @param connectionProperties
    * @param monitor
    */
   public RecordLoaderAggregate(final ConcurrentLinkedQueue<T> buffer,
         final Timestamp[] startTimes, final Properties connectionProperties,
         final Object monitor) {
      super(buffer, connectionProperties, monitor);
      this.startTimes = startTimes;

   }

   @SuppressWarnings("deprecation")
   public void run() {
      try {
         ResultSet rs = getDBConnection().retrieveAggregates(startTimes);

         // The stream threads need to woken up only for the first database
         // load..
         if (wakeFlag) {
            synchronized (monitor) {
               LOGGER.info("Waking the streamer threads..");
               Thread.sleep(1000);
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
            getBuffer().add((T) bean);
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
      } catch (InterruptedException e) {
         LOGGER.error("Error while waiting for initial database load..", e);
      }

   }
}
