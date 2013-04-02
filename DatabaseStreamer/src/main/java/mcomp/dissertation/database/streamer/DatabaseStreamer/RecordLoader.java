package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.database.streamer.beans.HistoryBean;

import org.apache.log4j.Logger;

/**
 * This class is responsible for loading an optimal number of records to the
 * buffer for streaming.
 */
public class RecordLoader<T> extends AbstractLoader<T> {
   private Timestamp startTime;
   private Timestamp endTime;
   private int loopCount;
   private int numberOfArchiveStreams;
   private static final Logger LOGGER = Logger.getLogger(RecordLoader.class);

   /**
    * 
    * @param buffer
    * @param startTime
    * @param connectionProperties
    * @param monitor
    * @param loopCount
    * @param numberofArchiveStreams
    * @param streamOption
    */
   public RecordLoader(final ConcurrentLinkedQueue<T> buffer,
         final long startTime, final Properties connectionProperties,
         final Object monitor, final int loopCount,
         final int numberofArchiveStreams, final int streamOption) {
      super(buffer, connectionProperties, monitor, streamOption);
      this.startTime = new Timestamp(startTime);
      this.endTime = new Timestamp(startTime + REFRESH_INTERVAL);
      this.loopCount = loopCount;
      this.numberOfArchiveStreams = numberofArchiveStreams;

   }

   @SuppressWarnings("unchecked")
   public void run() {
      try {
         ResultSet rs = dbconnect.retrieveWithinTimeStamp(startTime, endTime);
         if (loopCount == numberOfArchiveStreams && wakeFlag) {
            synchronized (monitor) {
               LOGGER.info("Waking the streamer threads..");
               Thread.sleep(1000);
               monitor.notifyAll();
            }
         }

         while (rs.next()) {
            long linkID = rs.getInt(1);
            float speed = rs.getFloat(2);
            int volume = rs.getInt(3);
            Timestamp ts = rs.getTimestamp(4);
            getBuffer().add((T) new HistoryBean(volume, speed, linkID, ts));
         }

         // Update the time stamps for the next fetch.
         long start = startTime.getTime() + REFRESH_INTERVAL;
         long end = endTime.getTime() + REFRESH_INTERVAL;
         startTime = new Timestamp(start);
         endTime = new Timestamp(end);
         wakeFlag = false;
      } catch (SQLException e) {
         LOGGER.error("Error accessing the database to retrieve archived data",
               e);
      } catch (InterruptedException e) {
         LOGGER.error("Error waking the sleeping threads", e);
      }

   }

}
