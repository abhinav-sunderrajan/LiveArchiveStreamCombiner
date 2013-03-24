package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.database.streamer.beans.HistoryAggregateBean;
import mcomp.dissertation.database.streamer.beans.LiveBean;
import mcomp.dissertation.database.streamer.listenersandsubscribers.FinalSubscriber;

import org.apache.log4j.Logger;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class StreamerCore2 {

   private long startTime;
   private EPServiceProvider[] cepJoin;
   private EPAdministrator[] cepAdmJoin;
   private Configuration[] cepConfigJoin;
   private EPRuntime[] cepRTJoin;
   private static ScheduledExecutorService executor;
   private static AggregateStreamer streamer;
   private static ScheduledFuture<?> future;
   private static Properties connectionProperties;
   private static DateFormat df;
   private static AtomicInteger streamRate;
   private static long dbLoadRate;
   private static Properties configProperties;
   private static int numberOfArchiveStreams;
   private static Object monitor;
   private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";
   private static final String CONNECTION_FILE_PATH = "src/main/resources/connection.properties";
   private static final int ARCHIVE_STREAM_COUNT = 6;
   private static int numberOfOperators;
   private static final Logger LOGGER = Logger.getLogger(StreamerCore2.class);

   /**
    * @param configFilePath
    * @param connectionFilePath Instantiate all the required settings and start
    * the archive data stream threads.
    */
   private StreamerCore2(final String configFilePath,
         final String connectionFilePath) {
      try {

         connectionProperties = new Properties();
         configProperties = new Properties();
         configProperties.load(new FileInputStream(configFilePath));
         monitor = new Object();
         connectionProperties.load(new FileInputStream(connectionFilePath));
         executor = Executors
               .newScheduledThreadPool(3 * numberOfArchiveStreams);
         streamRate = new AtomicInteger(Integer.parseInt(configProperties
               .getProperty("live.stream.rate.in.microsecs")));
         numberOfOperators = Integer.parseInt(configProperties
               .getProperty("number.of.operators"));
         dbLoadRate = (long) (streamRate.get() * 0.025);
         df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
         startTime = df.parse(
               configProperties.getProperty("archive.stream.start.time"))
               .getTime();

         // Instantiate the Esper parameter arrays

         cepJoin = new EPServiceProvider[numberOfOperators];
         cepAdmJoin = new EPAdministrator[numberOfOperators];
         cepConfigJoin = new Configuration[numberOfOperators];
         cepRTJoin = new EPRuntime[numberOfOperators];

         // Begin Esper Configuration for the join.
         for (int count = 0; count < numberOfOperators; count++) {
            cepConfigJoin[count] = new Configuration();
            cepConfigJoin[count].getEngineDefaults().getThreading()
                  .setListenerDispatchPreserveOrder(false);
            cepJoin[count] = EPServiceProviderManager.getProvider(
                  "JOINPROVIDER_" + count, cepConfigJoin[count]);
            cepConfigJoin[count].addEventType("LTALINKBEAN_" + count,
                  LiveBean.class.getName());
            cepConfigJoin[count].addEventType("ARCHIVEAGGREGATEBEAN_" + count,
                  HistoryAggregateBean.class.getName());
            cepConfigJoin[count].getEngineDefaults().getViewResources()
                  .setShareViews(false);
            cepRTJoin[count] = cepJoin[count].getEPRuntime();
            cepAdmJoin[count] = cepJoin[count].getEPAdministrator();
            EPStatement cepStatement = cepAdmJoin[count]
                  .createEPL("select * from mcomp.dissertation.database.streamer.beans.LiveBean"
                        + ".win:length(10000) as live inner join mcomp.dissertation.database.streamer"
                        + ".beans.HistoryAggregateBean.win:length(15000) as historyAgg"
                        + "  on historyAgg.linkId=live.linkId and historyAgg.mins=live.timeStamp.`minutes` and historyAgg.hrs=live.timeStamp.`hours`");
            // cepStatement.addListener(new FinalListener());
            cepStatement.setSubscriber(new FinalSubscriber());
         }
         // End of Esper configuration for the join

      } catch (ParseException e) {
         LOGGER.error(
               "Unable to determine the start date/stream rate from config file. Please check it",
               e);

      } catch (FileNotFoundException e) {
         LOGGER.error("Unable to find the config/connection properties files",
               e);
      } catch (IOException e) {
         LOGGER.error("Properties file contains non unicode values ", e);
      }

   }

   /**
    * @param args
    */
   public static void main(final String[] args) {

      String configFilePath;
      String connectionFilePath;
      if (args.length < 3) {
         configFilePath = CONFIG_FILE_PATH;
         connectionFilePath = CONNECTION_FILE_PATH;
         numberOfArchiveStreams = ARCHIVE_STREAM_COUNT;

      } else {
         configFilePath = args[0];
         connectionFilePath = args[1];
         numberOfArchiveStreams = Integer.parseInt(args[2]);

      }
      try {
         StreamerCore2 streamerCore;
         SigarSystemMonitor sysMonitor = SigarSystemMonitor.getInstance();
         streamerCore = new StreamerCore2(configFilePath, connectionFilePath);

         // Start monitoring the system CPU, memory parameters
         executor.scheduleAtFixedRate(sysMonitor, 0, 20, TimeUnit.SECONDS);

         streamerCore.setUpArchiveStreams();
         LiveTrafficStreamer live = new LiveTrafficStreamer(
               streamerCore.cepRTJoin, streamRate, df, monitor, executor);
         ScheduledFuture<?> liveFuture = live.startStreaming();
         // StreamRateChanger change = new StreamRateChanger(streamRate,
         // streamers, futures, executor, liveFuture, live.getRunnable());
         // executor.scheduleAtFixedRate(change, 0, 20, TimeUnit.SECONDS);
      } catch (InterruptedException ex) {
         LOGGER.error("The live streamer thread interrupted", ex);

      }

   }

   /**
    * This section is responsible for the settings to aggregate all the archived
    * sub data streams to create a single stream consisting of the average of
    * all.Create separate Esper aggregate listener for all archive sub streams
    * streams. This is not be confused with the instance variable settings which
    * finally combines the live stream with the the aggregate stream.
    */

   private void setUpArchiveStreams() throws InterruptedException {
      @SuppressWarnings("unchecked")
      ConcurrentLinkedQueue<HistoryAggregateBean> buffer = new ConcurrentLinkedQueue<HistoryAggregateBean>();
      Timestamp[] ts = new Timestamp[numberOfArchiveStreams];
      for (int count = 0; count < numberOfArchiveStreams; count++) {
         ts[count] = new Timestamp(startTime);
         startTime = startTime + 24 * 3600 * 1000;
      }

      // retrieve records from the database for every 25,000 records from the
      // live stream. This really depends upon the nature of the live
      // stream..
      RecordLoader2 loader = new RecordLoader2(buffer, ts,
            connectionProperties, monitor);
      executor.scheduleAtFixedRate(loader, 0, dbLoadRate, TimeUnit.SECONDS);

      // The most critical section of the program launching all the threads
      // which are needed. I also need to ensure all the archive streamer
      // threads are waiting on the last loader thread.

      streamer = new AggregateStreamer(buffer, cepRTJoin, monitor, executor,
            streamRate, Float.parseFloat(configProperties
                  .getProperty("archive.stream.rate.param")));
      future = streamer.startStreaming();

   }

}
