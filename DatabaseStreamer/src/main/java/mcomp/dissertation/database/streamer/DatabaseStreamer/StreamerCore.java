package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

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
public class StreamerCore {

   private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";
   private static final String CONNECTION_FILE_PATH = "src/main/resources/connection.properties";
   private static final int ARCHIVE_STREAM_COUNT = 6;
   private static final Logger LOGGER = Logger.getLogger(StreamerCore.class);
   private static DateFormat df;
   private EPServiceProvider cep;
   private static EPRuntime cepRT;
   private EPAdministrator cepAdm;
   private Configuration cepConfig;
   private static int streamRate;
   private long startTime;
   private static Properties connectionProperties;
   private static Properties configProperties;
   private static int numberOfArchiveStreams;
   private static Object monitor;

   /**
    * @param args
    */
   public static void main(String[] args) {

      String configFilePath;
      String connectionFilePath;
      if (args.length < 3) {
         configFilePath = CONFIG_FILE_PATH;
         connectionFilePath = CONNECTION_FILE_PATH;
         numberOfArchiveStreams = ARCHIVE_STREAM_COUNT;

      } else {
         configFilePath = args[0];
         connectionFilePath = args[1];
         numberOfArchiveStreams = Integer.parseInt(args[3]);

      }
      try {
         StreamerCore streamerCore;
         streamerCore = new StreamerCore(configFilePath, connectionFilePath);
         streamerCore.setUpArchiveStreams(numberOfArchiveStreams,
               connectionProperties);
         LiveStreamer live = new LiveStreamer(cepRT, streamRate, df, monitor);
         live.setName("live");
         live.start();
      } catch (InterruptedException ex) {
         LOGGER.error("The live streamer thread interrupted", ex);

      }

   }

   /**
    * @param configFilePath
    * @param connectionFilePath Instantiate all the required settings and start
    * the archive data stream threads.
    */
   public StreamerCore(String configFilePath, String connectionFilePath) {
      try {

         connectionProperties = new Properties();
         configProperties = new Properties();
         configProperties.load(new FileInputStream(configFilePath));
         monitor = new Object();
         connectionProperties.load(new FileInputStream(connectionFilePath));

         cepConfig = new Configuration();
         cep = EPServiceProviderManager.getProvider("PROVIDER", cepConfig);
         cepConfig.addEventType("LTALINKBEAN", LiveBean.class.getName());
         cepConfig.addEventType("ARCHIVEAGGREGATEBEAN",
               HistoryBean.class.getName());
         cepRT = cep.getEPRuntime();
         cepAdm = cep.getEPAdministrator();
         df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
         EPStatement cepStatement = cepAdm
               .createEPL("select live.linkId,live.avgSpeed,live.timeStamp,"
                     + "historyAgg.aggregateSpeed,historyAgg.linkId, historyAgg.aggregateVolume "
                     + "from mcomp.dissertation.database.streamer.beans.LiveBean.std:unique(linkId) as "
                     + "live, mcomp.dissertation.database.streamer.beans.HistoryAggregateBean.std:unique(linkId) "
                     + "as historyAgg where historyAgg.linkId=live.linkId");
         cepStatement.addListener(new FinalListener());
         startTime = df.parse(
               configProperties.getProperty("archive.stream.start.time"))
               .getTime();
         streamRate = Integer.parseInt(configProperties
               .getProperty("live.stream.rate.in.ms"));

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

   /*
    * This section is responsible for the settings to aggregate all the archived
    * sub data streams to create a single stream consisting of the average of
    * all.
    */

   private void setUpArchiveStreams(int numberOfArchiveStreams,
         Properties connectionProperties) throws InterruptedException {

      /*
       * Create separate Esper setting for all archive streams. This is not be
       * confused with the instance variable settings which finally combines the
       * live stream with the the aggregate stream.
       */
      EPServiceProvider cepAggregate;
      EPRuntime cepRTAggregate;
      EPAdministrator cepAdmAggregate;
      Configuration cepConfigAggregate;
      cepConfigAggregate = new Configuration();
      cepAggregate = EPServiceProviderManager
            .getProvider("PROVIDER", cepConfig);
      cepConfigAggregate.addEventType("ARCHIVESUBBEAN",
            HistoryBean.class.getName());
      cepAdmAggregate = cepAggregate.getEPAdministrator();
      EPStatement cepStatementAggregate = cepAdmAggregate
            .createEPL("select  count(*) as countRec, avg(volume) as avgVolume, avg(speed) as avgSpeed, linkId from "
                  + "mcomp.dissertation.database.streamer.beans.HistoryBean.std:groupwin(linkId).win:length(6) group by linkId HAVING count(*) = "
                  + numberOfArchiveStreams);
      cepStatementAggregate.addListener(new AggregateListener(cepRT));
      cepRTAggregate = cepAggregate.getEPRuntime();

      for (int count = 0; count < numberOfArchiveStreams; count++) {

         // Create a shared buffer between the thread retrieving records from
         // the database and the the thread streaming those records.
         ConcurrentLinkedQueue<HistoryBean> buffer = new ConcurrentLinkedQueue<HistoryBean>();

         // The most critical section of the program launching all the threads
         // which are needed;
         RecordLoader loader = new RecordLoader(buffer, startTime,
               connectionProperties);
         RecordStreamer streamer = new RecordStreamer(buffer, streamRate,
               cepRTAggregate, monitor);
         loader.setName("LOADER_"+count);
         streamer.setName("STREAMER_" + count);
         streamer.setDaemon(true);
         loader.setDaemon(true);
         loader.start();
         streamer.start();
         
         //Start the next archive stream for the records exactly a day after
         startTime = startTime + 24 * 3600 * 1000;

      }

   }

}
