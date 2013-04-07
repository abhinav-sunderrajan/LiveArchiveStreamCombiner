package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.beans.HistoryAggregateBean;
import mcomp.dissertation.beans.HistoryBean;
import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.database.streamer.listenersandsubscribers.AggregateSubscriber;
import mcomp.dissertation.database.streamer.listenersandsubscribers.FinalSubscriber;
import mcomp.dissertation.helper.CommonHelper;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

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
public final class StreamProcessor {
   private long startTime;
   private EPServiceProvider[] cepJoin;
   private EPAdministrator[] cepAdmJoin;
   private Configuration[] cepConfigJoin;
   private EPRuntime[] cepRTJoin;
   private static ScheduledExecutorService executor;
   private static Properties connectionProperties;
   private static DateFormat df;
   private static AtomicInteger streamRate;
   private static long dbLoadRate;
   private static long windowSize;
   private static Properties configProperties;
   private static int numberOfArchiveStreams;
   private static Object monitor;
   private static int numberOfAggregateOperators;
   private static int numberOfJoinOperators;
   private static int streamOption;
   private static SAXReader reader;
   private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";
   private static final String CONNECTION_FILE_PATH = "src/main/resources/connection.properties";
   private static final String XML_FILE_PATH = "src/main/resources/livestreams.xml";
   private static final int ARCHIVE_STREAM_COUNT = 6;
   private static final Logger LOGGER = Logger.getLogger(StreamProcessor.class);

   /**
    * @param configFilePath
    * @param connectionFilePath Instantiate all the required settings and start
    * the archive data stream threads.
    */
   private StreamProcessor(final String configFilePath,
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
         numberOfAggregateOperators = Integer.parseInt(configProperties
               .getProperty("number.of.aggregateoperators"));
         numberOfJoinOperators = Integer.parseInt(configProperties
               .getProperty("number.of.joinoperators"));
         streamOption = Integer.parseInt(configProperties
               .getProperty("archive.data.option"));
         reader = new SAXReader();
         dbLoadRate = (long) (streamRate.get() * Float
               .parseFloat(configProperties.getProperty("db.prefetch.rate")));
         windowSize = dbLoadRate / 2;
         df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
         startTime = df.parse(
               configProperties.getProperty("archive.stream.start.time"))
               .getTime();

         // Instantiate the Esper parameter arrays

         cepJoin = new EPServiceProvider[numberOfJoinOperators];
         cepAdmJoin = new EPAdministrator[numberOfJoinOperators];
         cepConfigJoin = new Configuration[numberOfJoinOperators];
         cepRTJoin = new EPRuntime[numberOfJoinOperators];

         // Begin Esper Configuration for the join.
         for (int count = 0; count < numberOfJoinOperators; count++) {
            cepConfigJoin[count] = new Configuration();
            cepConfigJoin[count].getEngineDefaults().getThreading()
                  .setListenerDispatchPreserveOrder(false);
            cepJoin[count] = EPServiceProviderManager.getProvider(
                  "JOINPROVIDER_" + count, cepConfigJoin[count]);
            cepConfigJoin[count].addEventType("LTALINKBEAN_" + count,
                  LiveTrafficBean.class.getName());
            cepConfigJoin[count].addEventType("ARCHIVEAGGREGATEBEAN_" + count,
                  HistoryAggregateBean.class.getName());
            cepConfigJoin[count].getEngineDefaults().getViewResources()
                  .setShareViews(false);
            cepRTJoin[count] = cepJoin[count].getEPRuntime();
            cepAdmJoin[count] = cepJoin[count].getEPAdministrator();
            CommonHelper helper = CommonHelper.getHelperInstance();
            EPStatement cepStatement = cepAdmJoin[count].createEPL(helper
                  .getJoinQuery(streamOption, dbLoadRate));
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
   @SuppressWarnings("unchecked")
   public static void main(final String[] args) {

      String configFilePath;
      String connectionFilePath;
      String xmlFilePath;
      if (args.length < 4) {
         configFilePath = CONFIG_FILE_PATH;
         connectionFilePath = CONNECTION_FILE_PATH;
         numberOfArchiveStreams = ARCHIVE_STREAM_COUNT;
         xmlFilePath = XML_FILE_PATH;

      } else {
         configFilePath = args[0];
         connectionFilePath = args[1];
         numberOfArchiveStreams = Integer.parseInt(args[2]);
         xmlFilePath = args[3];

      }
      try {
         StreamProcessor streamerCore;
         streamerCore = new StreamProcessor(configFilePath, connectionFilePath);

         // Start monitoring the system CPU, memory parameters
         SigarSystemMonitor sysMonitor = SigarSystemMonitor.getInstance();
         sysMonitor.setCpuUsageScalefactor((Double.parseDouble(configProperties
               .getProperty("cpu.usage.scale.factor"))));
         executor.scheduleAtFixedRate(sysMonitor, 0, 60, TimeUnit.SECONDS);

         // Depending upon the mode chosen aggregate at the database or
         // aggregate the sub-streams using the Esper engine.
         if (streamOption == 1) {
            LOGGER.info("Creating esper aggregating operator array. MODE 1");
            streamerCore.setUpArchiveSubStreams();
         } else {
            LOGGER.info("Aggregating archive data in the database itself. MODE 2");
            streamerCore.setUpAggregatedArchiveStream();
         }

         // Start streaming the live data.
         reader = new SAXReader();
         InputStream streamxml = new FileInputStream(xmlFilePath);
         reader = new SAXReader();
         Document doc = reader.read(streamxml);
         Element docRoot = doc.getRootElement();
         List<Element> streams = docRoot.elements();
         for (Element stream : streams) {
            int serverPort = Integer.parseInt(stream.attribute(1).getText());
            String streamName = stream.attribute(0).getText();
            if (streamName.equalsIgnoreCase("traffic")) {
               ConcurrentLinkedQueue<LiveTrafficBean> buffer = new ConcurrentLinkedQueue<LiveTrafficBean>();
               GenericLiveStreamer<LiveTrafficBean> streamer = new GenericLiveStreamer<LiveTrafficBean>(
                     buffer, streamerCore.cepRTJoin, monitor, executor,
                     streamRate, df, serverPort);
               streamer.startStreaming();
            } else {
               // ConcurrentLinkedQueue<LiveWeatherBean> buffer = new
               // ConcurrentLinkedQueue<LiveWeatherBean>();
               // GenericLiveStreamer<LiveWeatherBean> streamer = new
               // GenericLiveStreamer<LiveWeatherBean>(
               // buffer, streamerCore.cepRTJoin, monitor, executor,
               // streamRate, df, serverPort);
               // streamer.startStreaming();

            }

         }

         // StreamRateChanger change = new StreamRateChanger(streamRate,
         // streamers, futures, executor, liveFuture, live.getRunnable());
         // executor.scheduleAtFixedRate(change, 0, 20, TimeUnit.SECONDS);
      } catch (InterruptedException ex) {
         LOGGER.error("The live streamer thread interrupted", ex);

      } catch (FileNotFoundException e) {
         LOGGER.error("Unable to find xml file containing stream info", e);
         e.printStackTrace();
      } catch (DocumentException e) {
         LOGGER.error("Erroneous stream info xml file. Please check", e);
      }

   }

   /**
    * 
    * @throws InterruptedException There are two modes of running the program as
    * configured in the properties file. This option creates an array of
    * operators to aggregate all the archive sub-streams to produce a single
    * archive stream to be joined with the live stream.
    */

   @SuppressWarnings("rawtypes")
   private void setUpArchiveSubStreams() throws InterruptedException {

      // Initialize the local variables
      EPServiceProvider[] cepAggregateArray = new EPServiceProvider[numberOfAggregateOperators];
      EPRuntime[] cepRTAggregateArray = new EPRuntime[numberOfAggregateOperators];
      EPAdministrator[] cepAdmAggregateArray = new EPAdministrator[numberOfAggregateOperators];
      Configuration[] cepConfigAggregateArray = new Configuration[numberOfAggregateOperators];
      GenericArchiveStreamer[] streamers = new GenericArchiveStreamer[numberOfArchiveStreams];
      ScheduledFuture<?>[] futures = new ScheduledFuture[numberOfArchiveStreams];

      // Configuration settings begin for Aggregation
      for (int count = 0; count < numberOfAggregateOperators; count++) {
         cepConfigAggregateArray[count] = new Configuration();
         cepConfigAggregateArray[count].getEngineDefaults().getThreading()
               .setListenerDispatchPreserveOrder(false);
         cepConfigAggregateArray[count].getEngineDefaults().getViewResources()
               .setShareViews(false);
         cepConfigAggregateArray[count].addEventType("ARCHIVESUBBEAN_" + count,
               HistoryBean.class.getName());
         // End of configuration settings

         // Create instance of Esper engine. Create as many instances as the
         // number of aggregate operators required. Each identified by a
         // unique name.
         cepAggregateArray[count] = EPServiceProviderManager.getProvider(
               "PROVIDER_" + count, cepConfigAggregateArray[count]);

         cepAdmAggregateArray[count] = cepAggregateArray[count]
               .getEPAdministrator();
         // The statement is active on start to deactivate call the stop method.
         EPStatement cepStatementAggregate = cepAdmAggregateArray[count]
               .createEPL("@Hint('reclaim_group_aged="
                     + dbLoadRate
                     + ",') select  COUNT(*) as countRec, avg(volume) as avgVolume, avg(speed) as avgSpeed, linkId,readingMinutes as mins, "
                     + "readingHours as hrs from mcomp.dissertation.beans.HistoryBean.std:groupwin(linkId,readingMinutes,readingHours)"
                     + ".win:time_length_batch(" + windowSize
                     + "sec,6) group by linkId,readingMinutes,readingHours");

         // cepStatementAggregate.addListener(new AggregateListener(cepRT));
         cepStatementAggregate
               .setSubscriber(new AggregateSubscriber(cepRTJoin));
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
         streamers[count] = new GenericArchiveStreamer<HistoryBean>(
               buffer[count], cepRTAggregateArray, monitor, executor,
               streamRate, Float.parseFloat(configProperties
                     .getProperty("archive.stream.rate.param")));
         futures[count] = streamers[count].startStreaming();
      }

      for (int count = 1; count <= numberOfArchiveStreams; count++) {
         AbstractLoader<HistoryBean> loader = new RecordLoader<HistoryBean>(
               buffer[count - 1], startTime, connectionProperties, monitor,
               numberOfArchiveStreams, streamOption);

         // retrieve records from the database for every 30,000 records from the
         // live stream. This really depends upon the nature of the live
         // stream..
         executor.scheduleAtFixedRate(loader, 0, dbLoadRate, TimeUnit.SECONDS);
         // Start the next archive stream for the records exactly a day after
         startTime = startTime + 24 * 3600 * 1000;

      }

   }

   /**
    * This section contains the code for running the program in the second mode
    * where the burden of aggregating the archive data is pushed to the
    * database. Hence there are no aggregating Esper operators. The main
    * aggregated archive data stream is directly joined with the live stream.
    * @throws InterruptedException
    */

   @SuppressWarnings("rawtypes")
   private void setUpAggregatedArchiveStream() throws InterruptedException {
      ConcurrentLinkedQueue<HistoryAggregateBean> buffer = new ConcurrentLinkedQueue<HistoryAggregateBean>();
      GenericArchiveStreamer streamer;
      ScheduledFuture<?> future;
      Timestamp[] ts = new Timestamp[numberOfArchiveStreams];
      for (int count = 0; count < numberOfArchiveStreams; count++) {
         ts[count] = new Timestamp(startTime);
         startTime = startTime + 24 * 3600 * 1000;
      }

      streamer = new GenericArchiveStreamer<HistoryAggregateBean>(buffer,
            cepRTJoin, monitor, executor, streamRate,
            Float.parseFloat(configProperties
                  .getProperty("archive.stream.rate.param")));
      future = streamer.startStreaming();

      // retrieve records from the database for every 25,000 records from the
      // live stream. This really depends upon the nature of the live
      // stream..
      AbstractLoader<HistoryAggregateBean> loader = new RecordLoaderAggregate<HistoryAggregateBean>(
            buffer, ts, connectionProperties, monitor, streamOption);
      executor.scheduleAtFixedRate(loader, 0, dbLoadRate, TimeUnit.SECONDS);

   }

}
