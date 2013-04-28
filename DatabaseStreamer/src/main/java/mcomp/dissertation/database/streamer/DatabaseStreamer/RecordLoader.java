package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import mcomp.dissertation.beans.HistoryBean;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * This class is responsible for loading an optimal number of records to the
 * buffer for streaming.
 */
public class RecordLoader<T> extends AbstractLoader<T> {
   private Timestamp startTime;
   private Timestamp endTime;
   private int numberOfArchiveStreams;
   private boolean wakeFlag;
   private Polygon polygon;
   private ConcurrentHashMap<Long, Coordinate> linkIdCoord;
   private GeometryFactory gf;
   private static final Logger LOGGER = Logger.getLogger(RecordLoader.class);

   /**
    * 
    * @param buffer
    * @param startTime
    * @param connectionProperties
    * @param monitor
    * @param numberofArchiveStreams
    * @param streamOption
    * @param gf
    * @param polygon
    * @param linkIdCoord
    */
   public RecordLoader(final ConcurrentLinkedQueue<T> buffer,
         final long startTime, final Properties connectionProperties,
         final Object monitor, final int numberofArchiveStreams,
         final int streamOption, final GeometryFactory gf,
         final Polygon polygon,
         final ConcurrentHashMap<Long, Coordinate> linkIdCoord) {
      super(buffer, connectionProperties, monitor, streamOption);
      this.startTime = new Timestamp(startTime);
      this.endTime = new Timestamp(startTime + REFRESH_INTERVAL);
      this.numberOfArchiveStreams = numberofArchiveStreams;
      this.polygon = polygon;
      this.gf = gf;
      this.linkIdCoord = linkIdCoord;
      wakeFlag = true;

   }

   @SuppressWarnings("unchecked")
   public void run() {
      try {
         ResultSet rs = dbconnect.retrieveWithinTimeStamp(startTime, endTime);
         Coordinate coord;
         Point point;
         long linkid;
         while (rs.next()) {
            linkid = rs.getInt(1);
            coord = linkIdCoord.get(linkid);
            point = gf.createPoint(coord);
            // Load to the buffer if the location is within the polygon

            if (polygon.contains(point)) {
               long linkID = rs.getInt(1);
               float speed = rs.getFloat(2);
               int volume = rs.getInt(3);
               Timestamp ts = rs.getTimestamp(4);
               getBuffer().add((T) new HistoryBean(volume, speed, linkID, ts));
            }
         }

         if (wakeFlag) {
            synchronized (monitor) {
               LOGGER.info("Wait for live streams before further databse loading");
               monitor.wait();
               LOGGER.info("Receiving live streams. Start database load normally");
            }
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
