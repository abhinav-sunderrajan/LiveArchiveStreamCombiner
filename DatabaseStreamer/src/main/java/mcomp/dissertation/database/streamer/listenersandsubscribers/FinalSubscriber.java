package mcomp.dissertation.database.streamer.listenersandsubscribers;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import mcomp.dissertation.beans.HistoryAggregateBean;
import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.display.StreamJoinDisplay;

import org.apache.log4j.Logger;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;

/**
 * 
 * Registered subscriber joining the aggregated historic stream with the live
 * stream.
 * 
 */
public class FinalSubscriber {

   private DateFormat df;
   private long count = 0;
   private long numOfMsgsin30Sec = 0;
   private StreamJoinDisplay display;
   private long latency;
   private Map<Integer, Double> valueMap;
   private AtomicLong timer;
   private boolean throughputFlag;
   private FileWriter writeFile;
   private static final Logger LOGGER = Logger.getLogger(FinalSubscriber.class);

   @SuppressWarnings("deprecation")
   /**
    * 
    * @param writeFileDir
    * @param streamRate
    * @param imageSaveDirectory
    */
   public FinalSubscriber(final String writeFileDir, final int streamRate,
         final String imageSaveDirectory) {
      try {
         this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
         display = StreamJoinDisplay.getInstance("Join Performance Measure",
               imageSaveDirectory);
         timer = new AtomicLong(0);
         throughputFlag = true;
         display.addToDataSeries(new TimeSeries("Latency for Subscriber#"
               + this.hashCode() + " in msec", Minute.class),
               (1 + this.hashCode()));
         display.addToDataSeries(new TimeSeries(
               "Throughput/sec for Subscriber# " + this.hashCode(),
               Minute.class), (2 + this.hashCode()));
         valueMap = new HashMap<Integer, Double>();
         valueMap.put((2 + this.hashCode()), 0.0);
         valueMap.put((1 + this.hashCode()), 0.0);
         writeFile = new FileWriter(writeFileDir + "Throughput_"
               + Integer.toString(this.hashCode()) + "_"
               + Integer.toString(streamRate) + ".csv");
      } catch (IOException e) {
         LOGGER.error("Error creating file in the specified directory", e);
      }
   }

   /**
    * This method is called by Esper implicitly when registered the class is
    * registered as a subscriber. Care needs to be taken to match the order and
    * type of the parameters with that of the query.Failing which Esper throws
    * an error.
    * @param liveLinkId
    * @param liveSpeed
    * @param liveVolume
    * @param historyLinkid
    * @param historyAvgSpeed
    * @param historyAvgVolume
    * @param liveTimeStamp
    * @param evalTime
    */
   public void update(final Long liveLinkId, final Float liveSpeed,
         final Float liveVolume, final Long historyLinkid,
         final Double historyAvgSpeed, final Double historyAvgVolume,
         final Timestamp liveTimeStamp, final long evalTime) {
      try {
         if (throughputFlag) {
            timer.set(Calendar.getInstance().getTimeInMillis());
            numOfMsgsin30Sec = count;
         }
         count++;
         throughputFlag = false;
         if (count % 1000 == 0) {
            LOGGER.info("hashCode:" + this.hashCode() + " " + count + ":"
                  + df.format(Calendar.getInstance().getTime())
                  + " Linkid live and history(" + liveLinkId + "--"
                  + historyLinkid + "), speeds live and history(" + liveSpeed
                  + "--" + historyAvgSpeed + "), volume live and history("
                  + liveVolume + "--" + historyAvgVolume + ") Live Time stamp("
                  + liveTimeStamp + ")]");
         }

         // Refresh display values every 30 seconds
         if ((Calendar.getInstance().getTimeInMillis() - timer.get()) >= 30000) {
            double throughput = (1000 * (count - numOfMsgsin30Sec))
                  / (Calendar.getInstance().getTimeInMillis() - timer.get());
            latency = Calendar.getInstance().getTimeInMillis() - evalTime;
            valueMap.put((1 + this.hashCode()), latency / 1.0);
            valueMap.put((2 + this.hashCode()), throughput);
            display.refreshDisplayValues(valueMap);
            writeFile.append(Double.toString(throughput));
            writeFile.append("\n");
            writeFile.flush();
            throughputFlag = true;
         }
      } catch (IOException e) {
         LOGGER.error("Error writing to file", e);
      }

   }

   /**
    * Overloaded equivalent of the above update method.
    * @param live
    * @param history
    */
   public void update(LiveTrafficBean live, HistoryAggregateBean history) {
      count++;

      if (count % 1000 == 0) {
         if (history != null) {
            LOGGER.info("hashCode:" + this.hashCode() + " " + count + ":"
                  + df.format(Calendar.getInstance().getTime())
                  + " [Details(EventTime:" + live.getEventTime()
                  + " Linkid live and history(" + live.getLinkId() + "--"
                  + history.getLinkId() + "), speeds live and history("
                  + live.getSpeed() + "--" + history.getAggregateSpeed()
                  + "), volume live and history(" + live.getVolume() + "--"
                  + history.getAggregateVolume() + ") Live time stamp("
                  + live.getTimeStamp() + ")]");
         } else {

            LOGGER.info("hashCode:" + this.hashCode() + " " + count + ":"
                  + df.format(Calendar.getInstance().getTime())
                  + " [Details(EventTime:" + live.getEventTime()
                  + " Linkid live and history(" + live.getLinkId()
                  + "-- null ), speeds live and history(" + live.getSpeed()
                  + "-- null), volume live and history(" + live.getVolume()
                  + "-- null) Live time stamp(" + live.getTimeStamp() + ")]");

         }
      }

   }
}
