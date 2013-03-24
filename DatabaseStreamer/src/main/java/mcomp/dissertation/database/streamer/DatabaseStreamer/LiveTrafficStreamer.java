package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.database.streamer.beans.LiveBean;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPRuntime;

/**
 * This thread is responsible for streaming live data from a CSV file.
 */
public class LiveTrafficStreamer {

   private static final Logger LOGGER = Logger
         .getLogger(LiveTrafficStreamer.class);

   private File file;
   private BufferedReader br;
   private AtomicInteger streamRate;
   private DateFormat df;
   private DateFormat dfLocal;
   private ScheduledExecutorService executor;
   private Runnable runnable;

   /**
    * Initialize and start file streaming.
    * @param cepRTJoinArray
    * @param streamRate
    * @param monitor
    * @param executor
    */
   public LiveTrafficStreamer(final EPRuntime[] cepRTJoinArray,
         final AtomicInteger streamRate, final DateFormat df,
         final Object monitor, final ScheduledExecutorService executor) {
      try {
         this.streamRate = streamRate;
         this.df = df;
         this.dfLocal = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
         this.file = readLTALinkData();
         this.br = new BufferedReader(new FileReader(file));
         this.executor = executor;
         this.runnable = new Runnable() {
            private int count = 0;

            public void run() {
               try {
                  // Release the lock on the monitor lock to release all waiting
                  // threads.Applicable only for the first time.
                  if (count == 0) {
                     synchronized (monitor) {
                        LOGGER.info("Wait for the initial data base load before streaming..");
                        monitor.wait();
                        count++;
                        LOGGER.info("Awake!! Starting to streaming now");
                     }
                  }
                  if (br.ready()) {
                     LiveBean bean = parseLine(br.readLine());
                     bean.setEventTime(dfLocal.format(Calendar.getInstance()
                           .getTime()));
                     long bucket = bean.getLinkId() % cepRTJoinArray.length;
                     cepRTJoinArray[(int) bucket].sendEvent(bean);
                     count++;
                     // print for evaluation purposes only..
                     // if (count % 1000 == 0) {
                     // LOGGER.info(count
                     // + " "
                     // + bean.getLinkId()
                     // + " at "
                     // + bean.getTimeStamp().getMinutes()
                     // + " "
                     // + dfLocal
                     // .format(Calendar.getInstance().getTime()));
                     // }
                  }
               } catch (EPException e) {
                  LOGGER.error("Error sending event to listener", e);
               } catch (IOException e) {
                  LOGGER.error("Error reading CSV file", e);
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }

            }
         };
      } catch (Exception e) {
         LOGGER.error(e.getMessage(), e);

      }

   }

   public ScheduledFuture<?> startStreaming() {
      ScheduledFuture<?> liveFuture = null;
      liveFuture = executor.scheduleAtFixedRate(runnable, 0, streamRate.get(),
            TimeUnit.MICROSECONDS);
      return liveFuture;

   }

   /**
    * @param properties
    * @return Parser
    * @throws Exception
    */
   private File readLTALinkData() throws Exception {
      File dir = new File(
            "C:\\Users\\Usha Sundarajan\\Documents\\ProjectData\\dummy data\\");
      LOGGER.info("Reading live data from " + dir.getAbsolutePath());
      File[] files = dir.listFiles(new FileFilter() {

         public boolean accept(final File pathname) {
            String n = pathname.getName();

            if (n.startsWith(".") || (!n.endsWith(".csv"))) {
               return false;
            } else {

               @SuppressWarnings("deprecation")
               Date dataDate = new Date("2011/04/17");

               Date fileDate = null;
               try {
                  fileDate = getDateFromFileName(n.substring(15, 25));
               } catch (Exception e) {
                  e.printStackTrace();
               }
               if (dataDate.equals(fileDate)) {
                  return true;
               } else {
                  return false;
               }

            }
         }

      });
      if (files != null) {
         return files[0];
      } else {
         throw new Exception(
               "Unable to initialize LTA Link cache - check directory path.");
      }

   }

   @SuppressWarnings("deprecation")
   private Date getDateFromFileName(final String dateString) throws Exception {
      try {

         String date = dateString.substring(0, 4) + "/"
               + dateString.substring(5, 7) + "/" + dateString.substring(8, 10);

         return new Date(date);
      } catch (Exception e) {
         throw new Exception(e);
      }
   }

   private LiveBean parseLine(final String line) {
      LiveBean bean = new LiveBean();
      try {

         String[] items = line.split("\\|");
         bean.setLinkId(Integer.parseInt(items[0].trim()));

         Date time;
         time = df.parse(items[1].trim());
         bean.setTimeStamp(new Timestamp(time.getTime()));
         // Check if speed is null
         if (items[2].trim().equals("") || items[2].trim() == null) {
            bean.setAvgSpeed(0);
         } else {
            bean.setAvgSpeed(Float.parseFloat(items[2].trim()));

         }
         // Check if volume is null
         if (items[3].trim().equals("") || items[3].trim() == null) {
            bean.setAvgVolume(0);
         } else {
            bean.setAvgVolume(Integer.parseInt(items[3].trim()));
         }

      } catch (Exception e) {
         e.printStackTrace();
      }

      return bean;

   }

   /**
    * @return the runnable
    */
   public Runnable getRunnable() {
      return runnable;
   }

}
