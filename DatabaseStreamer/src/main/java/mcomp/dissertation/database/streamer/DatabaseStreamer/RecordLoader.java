package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.database.streamer.RDBMSAccess.DBConnect;
import mcomp.dissertation.database.streamer.beans.HistoryBean;

import org.apache.log4j.Logger;

/**
 * This class is responsible for loading an optimal number of records to the
 * buffer for streaming.
 * 
 */
public class RecordLoader extends Thread {
   private Queue<HistoryBean> buffer;
   private Timestamp startTime;
   private Timestamp endTime;
   private DBConnect dbconnect;
   private static long INTERVAL = 300000;
   private static final Logger LOGGER = Logger.getLogger(RecordLoader.class);
   /**
    * @param buffer
    * @param startTime
    * @param connectionProperties
    */
   public RecordLoader(ConcurrentLinkedQueue<HistoryBean> buffer,
         long startTime, Properties connectionProperties) {
      this.buffer = buffer;
      this.startTime = new Timestamp(startTime);
      this.endTime = new Timestamp(startTime + INTERVAL);
      dbconnect=new DBConnect();
      dbconnect.openDBConnection(connectionProperties);

   }

   @Override
   public void run() {
       while (true) {
          try {
         ResultSet rs = dbconnect.retrieveWithinTimeStamp(startTime, endTime);         
            while (rs.next()) {
               long linkID = rs.getInt(1);
               float speed = rs.getFloat(2);
               int volume = rs.getInt(3);
               Timestamp ts = rs.getTimestamp(4);
               buffer.add(new HistoryBean(volume, speed, linkID, ts));
            }

            // Update the time stamps for the next fetch.
            long start = startTime.getTime() + INTERVAL;
            long end = endTime.getTime() + INTERVAL;
            startTime = new Timestamp(start);
            endTime = new Timestamp(end);
            Thread.sleep(INTERVAL-50000);
         } catch (SQLException e) {
            LOGGER.error(
                  "Error accessing the database to retrieve archived data", e);
         } catch (InterruptedException e) {
            LOGGER.error("Interrupted exception", e);
         }

      }

   }
}
