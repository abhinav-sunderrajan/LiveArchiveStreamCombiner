package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.Date;

import mcomp.dissertation.database.streamer.beans.LiveBean;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

/**
 * This thread is responsible for streaming live data from a CSV file.
 * 
 */
public class LiveStreamer extends Thread {

   private static final Logger LOGGER = Logger.getLogger(LiveStreamer.class);

   private File file;
   private BufferedReader br;
   private EPRuntime cepRT;
   private int streamRate;
   private DateFormat df;
   private Object monitor;

   /**
    * Initialize and start file streaming.
    * @param cepRT
    * @param streamRate
    * @param monitor
    */
   public LiveStreamer(EPRuntime cepRT, int streamRate, DateFormat df,
         Object monitor) {
      try {
         this.cepRT = cepRT;
         this.streamRate = streamRate;
         this.df = df;
         this.file = readLTALinkData();
         this.monitor = monitor;
         this.br = new BufferedReader(new FileReader(file));
      } catch (Exception e) {
         LOGGER.error(e.getMessage(), e);

      }

   }

   @Override
   public void run() {
      try {

         // Release the lock on the monitor lock to release all waiting threads.
         synchronized (monitor) {
            monitor.notifyAll();
         }
         while (br.ready()) {
            LiveBean bean;
            bean = parseLine(br.readLine());
            cepRT.sendEvent(bean);
            Thread.sleep(streamRate);
         }
      } catch (IOException e) {
         LOGGER.error("Unable to read record from CSV file..", e);

      } catch (InterruptedException e) {
         LOGGER.error("Live streamer thread interrupted", e);
      }

   }

   /**
    * @param properties
    * @return Parser
    * @throws Exception
    */
   private File readLTALinkData() throws Exception {
      File dir = new File(
            "C:\\Users\\Usha Sundarajan\\Documents\\ProjectData\\dummy data\\");
      LOGGER.info("Reading live data from "+ dir.getAbsolutePath());
      File[] files = dir.listFiles(new FileFilter() {

         public boolean accept(File pathname) {
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

   private LiveBean parseLine(String line) {
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

}
