package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.display.StreamJoinDisplay;
import mcomp.dissertation.helper.CommonHelper;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.ConfigurationDBRef;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

public class EsperSQLTest {
   private static ScheduledExecutorService executor;
   private static ConcurrentHashMap<Long, Coordinate> linkIdCoord;
   private static final String CONFIG_FILE_PATH = "resources/config.properties";
   private static final String CONNECTION_FILE_PATH = "resources/connection.properties";
   private static final Logger LOGGER = Logger.getLogger(EsperSQLTest.class);
   private static final GeometryFactory gf = new GeometryFactory();

   public static void main(String[] args) {

      try {

         Properties connectionProperties = new Properties();
         Properties configProperties = new Properties();
         configProperties.load(new FileInputStream(CONFIG_FILE_PATH));
         Object monitor = new Object();
         connectionProperties.load(new FileInputStream(CONNECTION_FILE_PATH));
         Configuration cepConfig = makeDBSettings(connectionProperties);
         executor = Executors.newScheduledThreadPool(3);
         EPServiceProvider cep;
         EPAdministrator cepAdm;
         AtomicInteger streamRate = new AtomicInteger(
               Integer.parseInt(configProperties
                     .getProperty("live.stream.rate.in.microsecs")));

         cep = EPServiceProviderManager.getProvider("PROVIDER", cepConfig);
         cepConfig.addEventType("LIVETRAFFIC", LiveTrafficBean.class.getName());
         EPRuntime[] cepRTJoin = { cep.getEPRuntime() };

         cepAdm = cep.getEPAdministrator();
         EPStatement cepStatement = cepAdm
               .createEPL("select linkId,histLink, speed, history.histAvg,current_timestamp from mcomp.dissertation.beans.LiveTrafficBean,sql:A0092715"
                     + " [' select LINKID as histLink, AVG(SPEED) as histAvg from DataArchive where LINKID = ${linkId} "
                     + "AND TIME_STAMP IN (\"2011-04-11 00:00:00\",\"2011-04-12 00:00:00\")'] as history ");
         cepStatement.setSubscriber(new CEPListener(configProperties
               .getProperty("image.save.directory")));
         ConcurrentLinkedQueue<LiveTrafficBean> buffer = new ConcurrentLinkedQueue<LiveTrafficBean>();
         // Create the polygon for spatial filter
         WKTReader reader = new WKTReader(gf);
         Polygon polygon = (Polygon) reader.read(configProperties
               .getProperty("spatial.polygon"));
         linkIdCoord = new ConcurrentHashMap<Long, Coordinate>();
         CommonHelper helper = CommonHelper.getHelperInstance();

         File file = new File(
               configProperties.getProperty("linkid.cordinate.file"));
         BufferedReader br = new BufferedReader(new FileReader(file));

         helper.loadLinkIdCoordinates(linkIdCoord, file, br);
         LOGGER.info("Finished loading link ID coordinate information for "
               + linkIdCoord.size() + " link Ids to memory");
         GenericLiveStreamer<LiveTrafficBean> streamer = new GenericLiveStreamer<LiveTrafficBean>(
               buffer, cepRTJoin, monitor, executor, streamRate, null, 8080,
               gf, polygon, linkIdCoord, false,
               configProperties.getProperty("ingestion.file.dir"),
               configProperties.getProperty("image.save.directory"));
         streamer.startStreaming();

      } catch (Exception e) {
         LOGGER.error("Set the error right abhinav..", e);
      }

   }

   static class CEPListener {
      private StreamJoinDisplay display;
      private Map<Integer, Double> valueMap;
      private AtomicLong timer;
      private boolean throughputFlag;
      private long count = 0;
      private long numOfMsgsin30Sec = 0;
      private long latency;

      @SuppressWarnings("deprecation")
      public CEPListener(String imageSaveDirectory) {
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

      }

      public void update(Long linkId, Long histLink, Double speed,
            BigDecimal histAvg, Long evalTime) {

         if (throughputFlag) {
            timer.set(Calendar.getInstance().getTimeInMillis());
            numOfMsgsin30Sec = count;
         }
         count++;
         throughputFlag = false;
         if (count % 100 == 0) {
            System.out.println(linkId + " : " + histLink + " ::" + speed
                  + " : " + histAvg);
         }

         // Refresh display values every 30 seconds
         if ((Calendar.getInstance().getTimeInMillis() - timer.get()) >= 30000) {
            double throughput = (1000 * (count - numOfMsgsin30Sec))
                  / (Calendar.getInstance().getTimeInMillis() - timer.get());
            latency = Calendar.getInstance().getTimeInMillis() - evalTime;
            valueMap.put((1 + this.hashCode()), latency / 1.0);
            valueMap.put((2 + this.hashCode()), throughput);
            display.refreshDisplayValues(valueMap);
            throughputFlag = true;
         }

      }
   }

   /**
    * @return Configuration
    */
   public static Configuration makeDBSettings(Properties connectionProperties) {

      System.out.println("the database is "
            + connectionProperties.getProperty("database.name"));

      Properties props = new Properties();
      props.put("username",
            connectionProperties.getProperty("database.username"));
      props.put("password",
            connectionProperties.getProperty("database.password"));
      props.put("driverClassName", "com.mysql.jdbc.Driver");
      props.put("url", connectionProperties.getProperty("database.url"));
      props.put("initialSize", 2);
      props.put("validationQuery", "select 1 from dual");
      ConfigurationDBRef configDB = new ConfigurationDBRef();
      // BasicDataSourceFactory is an Apache DBCP import
      configDB.setDataSourceFactory(props,
            BasicDataSourceFactory.class.getName());
      configDB
            .setConnectionLifecycleEnum(ConfigurationDBRef.ConnectionLifecycleEnum.RETAIN);
      Configuration configuration = new Configuration();
      configuration.addDatabaseReference(
            connectionProperties.getProperty("database.name"), configDB);
      return configuration;

   }

}
