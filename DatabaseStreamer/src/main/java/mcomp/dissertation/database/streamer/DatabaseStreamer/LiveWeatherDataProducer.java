package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import mcomp.dissertation.beans.LiveWeatherBean;

import org.apache.log4j.Logger;

import com.espertech.esper.client.EPRuntime;

/**
 * 
 * Create pseudo stream representing a weather stream from all links. The rain
 * and temperature are averaged over every half an hour. The data from all links
 * are sent out to a streamer once every 30 minutes.
 * 
 */

public class LiveWeatherDataProducer {

   private Runnable runnableStreamer;
   private int streamRate;
   private Runnable runnableProducer;
   private Timestamp obsTime;
   private ScheduledExecutorService executor;
   private List<Long> linkIdList;
   private File file;
   private BufferedReader br;
   private static final Logger LOGGER = Logger
         .getLogger(LiveWeatherDataProducer.class);
   private ConcurrentLinkedQueue<LiveWeatherBean> buffer;

   /**
    * 
    * @param buffer1
    * @param cepRTAggregateArray
    * @param monitor
    * @param executor
    * @param streamRate
    * @throws ParseException
    * @throws FileNotFoundException
    */

   public LiveWeatherDataProducer(final EPRuntime cepRT, final Object monitor,
         final ScheduledExecutorService executor, final DateFormat df,
         final int streamRate) throws ParseException, FileNotFoundException {
      this.streamRate = streamRate;
      this.executor = executor;
      this.linkIdList = new ArrayList<Long>();
      obsTime = new Timestamp(df.parse("11-Apr-2011 00:00:00").getTime());

      file = new File("C:\\Users\\Usha Sundarajan\\Documents\\linkIds.csv");

      br = new BufferedReader(new FileReader(file));

      // This runnable sends the data to the esper aggregator for joining.

      this.runnableStreamer = new Runnable() {

         private int count = 0;
         private LiveWeatherBean weather;

         public void run() {

            // Release the lock on the monitor lock to release all waiting
            // threads. Applicable only for the first time.
            if (count == 0) {
               synchronized (monitor) {
                  LOGGER.info("Wait for the initial data base load before streaming..");
                  try {
                     monitor.wait();
                     LOGGER.info("Awake!! Starting to streaming now");
                  } catch (InterruptedException e) {
                     e.printStackTrace();
                  }
               }
            }

            while (buffer.isEmpty()) {
               // Poll till the producer has filled the queue. Bad approach will
               // optimize this.
            }

            weather = buffer.poll();
            cepRT.sendEvent(weather);
            count++;
            // // print for evaluation purposes only..
            // if (count % 1000 == 0) {
            // System.out.println(count + " " + streamRate.get() + " "
            // + dfLocal.format(Calendar.getInstance().getTime()));
            // }

         }
      };

      // This is pseudo weather data producer.

      this.runnableProducer = new Runnable() {
         private Random random = new Random();
         private LiveWeatherBean weather = new LiveWeatherBean();

         public void run() {

            Iterator<Long> it = linkIdList.iterator();
            while (it.hasNext()) {
               weather.setLinkId(it.next());
               weather.setTemperature(random.nextFloat() * 40);
               weather.setRain(random.nextFloat() * 10);
               weather.setTimeStamp(obsTime);
               buffer.add(weather);
            }
            obsTime = new Timestamp(obsTime.getTime() + 1800000);

         }
      };

   }

   /**
    * Random stream generator. Will be replaced in future
    * @throws IOException
    */
   private void setUpEnvironment() throws IOException {
      String linkId;

      // Get a list of all links from the CSV file
      while (true) {
         linkId = br.readLine();
         if (linkId == null) {
            break;
         } else {
            linkIdList.add(Long.parseLong(linkId));
         }
      }

      executor.scheduleAtFixedRate(runnableProducer, 0, 1800, TimeUnit.SECONDS);
   }

   /**
    * 
    * @return ScheduledFuture
    * @throws IOException This method starts the stream
    */

   public ScheduledFuture<?> startStreaming() throws IOException {

      setUpEnvironment();

      ScheduledFuture<?> archiveFuture = executor.scheduleAtFixedRate(
            runnableStreamer, 0, streamRate, TimeUnit.MICROSECONDS);
      return archiveFuture;
   }

   /**
    * @return the runnable
    */
   public Runnable getRunnable() {
      return runnableStreamer;
   }

}
