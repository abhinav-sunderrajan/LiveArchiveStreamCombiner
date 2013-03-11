package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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

import mcomp.dissertation.database.streamer.beans.HistoryBean;
import mcomp.dissertation.database.streamer.beans.LiveBean;
import mcomp.dissertation.databse.streamer.listeners.AggregateListener;
import mcomp.dissertation.databse.streamer.listeners.FinalListener;

import org.apache.log4j.Logger;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

/**
 * The main class which sets the ball rolling for the simulated live stream and
 * the archived streams.
 */
public final class StreamerCore {
   private EPServiceProvider cep;
   private static EPRuntime cepRT;
   private EPAdministrator cepAdm;
   private Configuration cepConfig;
   private static ScheduledExecutorService executor;
   private long startTime;
   private static RecordStreamer[] streamers;
   private static ScheduledFuture<?>[] futures;
   private static Properties connectionProperties;
   private static DateFormat df;
   private static AtomicInteger streamRate;
   private static Properties configProperties;
   private static int numberOfArchiveStreams;
   private static Object monitor;
   private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";
   private static final String CONNECTION_FILE_PATH = "src/main/resources/connection.properties";
   private static final int ARCHIVE_STREAM_COUNT = 6;
   private static final int NUMBER_OF_AGGREGATORS = 4;
   private static final Logger LOGGER = Logger.getLogger(StreamerCore.class);

   /**
    * @param configFilePath
    * @param connectionFilePath Instantiate all the required settings and start
    * the archive data stream threads.
    */
   private StreamerCore(final String configFilePath,
         final String connectionFilePath) {
      try {

         connectionProperties = new Properties();
         configProperties = new Properties();
         configProperties.load(new FileInputStream(configFilePath));
         monitor = new Object();
         connectionProperties.load(new FileInputStream(connectionFilePath));
         executor = Executors
               .newScheduledThreadPool(3 * numberOfArchiveStreams);
         cepConfig = new Configuration();
         cep = EPServiceProviderManager.getProvider("PROVIDER", cepConfig);
         cepConfig.addEventType("LTALINKBEAN", LiveBean.class.getName());
         cepConfig.addEventType("ARCHIVEAGGREGATEBEAN",
               HistoryBean.class.getName());
         cepRT = cep.getEPRuntime();
         cepAdm = cep.getEPAdministrator();
         streamRate = new AtomicInteger(Integer.parseInt(configProperties
               .getProperty("live.stream.rate.in.microsecs")));
         df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
         EPStatement cepStatement = cepAdm
               .createEPL("select live.linkId,live.avgSpeed,live.timeStamp,live.eventTime,"
                     + "historyAgg.aggregateSpeed,historyAgg.linkId, historyAgg.aggregateVolume from "
                     + " mcomp.dissertation.database.streamer.beans.LiveBean.std:unique(linkId,timeStamp.getMinutes()) as "
                     + "live left outer join mcomp.dissertation.database.streamer.beans.HistoryAggregateBean.std:unique(linkId,mins)"
                     + "as historyAgg on historyAgg.linkId=live.linkId "
                     + "where (historyAgg.mins=live.timeStamp.getMinutes() "
                     + " and historyAgg.hrs=live.timeStamp.getHours())");
         cepStatement.addListener(new FinalListener());
         startTime = df.parse(
               configProperties.getProperty("archive.stream.start.time"))
               .getTime();
         streamers = new RecordStreamer[numberOfArchiveStreams];
         futures = new ScheduledFuture[numberOfArchiveStreams];

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
         StreamerCore streamerCore;
         streamerCore = new StreamerCore(configFilePath, connectionFilePath);
         streamerCore.setUpArchiveStreams();
         LiveStreamer live = new LiveStreamer(cepRT, streamRate, df, monitor,
               executor);
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
      EPServiceProvider[] cepAggregateArray = new EPServiceProvider[NUMBER_OF_AGGREGATORS];
      EPRuntime[] cepRTAggregateArray = new EPRuntime[NUMBER_OF_AGGREGATORS];
      EPAdministrator[] cepAdmAggregateArray = new EPAdministrator[NUMBER_OF_AGGREGATORS];
      Configuration[] cepConfigAggregateArray = new Configuration[NUMBER_OF_AGGREGATORS];

      for (int count = 0; count < NUMBER_OF_AGGREGATORS; count++) {
         cepConfigAggregateArray[count] = new Configuration();
         cepAggregateArray[count] = EPServiceProviderManager.getProvider(
               "PROVIDER_" + count, cepConfigAggregateArray[count]);
         cepConfigAggregateArray[count].addEventType("ARCHIVESUBBEAN_" + count,
               HistoryBean.class.getName());
         cepAdmAggregateArray[count] = cepAggregateArray[count]
               .getEPAdministrator();
         EPStatement cepStatementAggregate = cepAdmAggregateArray[count]
               .createEPL("select  COUNT(*) as countRec, avg(volume) as avgVolume, avg(speed) as avgSpeed, linkId,readingMinutes as mins, "
                     + "readingHours as hrs from mcomp.dissertation.database.streamer.beans.HistoryBean.std:groupwin(linkId,readingMinutes,readingHours)"
                     + ".win:time_length_batch(40 sec,6) group by linkId,readingMinutes,readingHours having count(*)<6");
         cepStatementAggregate.addListener(new AggregateListener(cepRT));
         cepRTAggregateArray[count] = cepAggregateArray[count].getEPRuntime();

      }

      @SuppressWarnings("unchecked")
      ConcurrentLinkedQueue<HistoryBean>[] buffer = new ConcurrentLinkedQueue[numberOfArchiveStreams];

      // Create a shared buffer between the thread retrieving records from
      // the database and the the thread streaming those records.

      for (int count = 0; count < numberOfArchiveStreams; count++) {
         buffer[count] = new ConcurrentLinkedQueue<HistoryBean>();

      }

      // The most critical section of the program launching all the threads
      // which are needed. I also need to ensure all the archive streamer
      // threads are waiting on the last loader thread.

      for (int count = 0; count < numberOfArchiveStreams; count++) {
         streamers[count] = new RecordStreamer(buffer[count],
               cepRTAggregateArray, monitor, executor, streamRate);
         futures[count] = streamers[count].startStreaming();
      }

      for (int count = 1; count <= numberOfArchiveStreams; count++) {
         RecordLoader loader = new RecordLoader(buffer[count - 1], startTime,
               connectionProperties, monitor, count, numberOfArchiveStreams);
         executor.scheduleWithFixedDelay(loader, 0, 80, TimeUnit.SECONDS);
         // Start the next archive stream for the records exactly a day after
         startTime = startTime + 24 * 3600 * 1000;

      }

   }
}
