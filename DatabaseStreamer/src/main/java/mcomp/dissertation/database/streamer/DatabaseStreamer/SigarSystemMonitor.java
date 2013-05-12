package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mcomp.dissertation.display.GenericChartDisplay;

import org.apache.log4j.Logger;
import org.hyperic.sigar.Cpu;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;

/**
 * 
 * This thread will be responsible for monitoring and reporting the system
 * parameters at a regular interval. it makes use of the Sigar api.
 * 
 */
@SuppressWarnings("deprecation")
public class SigarSystemMonitor implements Runnable {
   private CpuInfo[] cpuinfo;
   private Cpu cpu;
   private Mem mem;
   private Sigar sigar;
   private static SigarSystemMonitor instance;
   private static final Logger LOGGER = Logger
         .getLogger(SigarSystemMonitor.class);
   private GenericChartDisplay display;
   private double cpuUsageScalefactor;
   private Map<Integer, Double> valueMap;
   private static FileWriter writeFile;

   /**
    * private constructor singleton pattern
    */

   private SigarSystemMonitor(String imageSaveDirectory) {
      sigar = new Sigar();
      // Settings for display. The code is ugly need to figure out a better way
      // of doing things here.
      display = new GenericChartDisplay("System Parameters", imageSaveDirectory);
      valueMap = new HashMap<Integer, Double>();
      display.addToDataSeries(new TimeSeries("Total Free Memory %",
            Minute.class), 1);
      valueMap.put(1, 0.0);
      display.addToDataSeries(
            new TimeSeries("JVM Free Memory %", Minute.class), 2);
      valueMap.put(2, 0.0);
      display.addToDataSeries(new TimeSeries("CPU kernel time scaled down",
            Minute.class), 3);
      valueMap.put(3, 0.0);

      // End of settings for display
      try {
         cpuinfo = sigar.getCpuInfoList();
         for (int i = 0; i < cpuinfo.length; i++) {
            Map map = cpuinfo[i].toMap();
            LOGGER.info("CPU " + i + ": " + map);
         }

      } catch (SigarException e) {
         LOGGER.error("Error in getting system information from sigar..", e);
      }

   }

   /**
    * return the instance for the SigarSystemMonitor class
    * @param memFileDir
    * @param streamRate
    * @param imageSaveDirectory
    * @return SigarSystemMonitor
    */

   public static SigarSystemMonitor getInstance(String memFileDir,
         int streamRate, String imageSaveDirectory) {
      if (instance == null) {
         instance = new SigarSystemMonitor(imageSaveDirectory);
         try {
            writeFile = new FileWriter(memFileDir + "FreeMemPercentage_"
                  + Integer.toString(streamRate) + ".csv");
         } catch (IOException e) {
            LOGGER.error("Error in path provided for memory file", e);
         }
      }

      return instance;

   }

   /**
    * 
    * @param cpuUsageScalefactor
    */
   public void setCpuUsageScalefactor(double cpuUsageScalefactor) {
      this.cpuUsageScalefactor = cpuUsageScalefactor;
   }

   public void run() {

      if (cpuUsageScalefactor == 0.0d) {
         cpuUsageScalefactor = 1000000.0;
      }

      try {
         cpu = sigar.getCpu();
         mem = sigar.getMem();
         long actualFree = mem.getActualFree();
         long actualUsed = mem.getActualUsed();
         long jvmFree = Runtime.getRuntime().freeMemory();
         long jvmTotal = Runtime.getRuntime().totalMemory();
         // Update display values and send to chart
         valueMap.put(1, mem.getFreePercent());
         valueMap.put(2, ((jvmFree * 100.0) / jvmTotal));
         valueMap.put(3, cpu.getSys() / cpuUsageScalefactor);
         display.refreshDisplayValues(valueMap);

         writeFile.append(Double.toString((jvmFree * 100.0) / jvmTotal));
         writeFile.append("\n");
         writeFile.flush();

         LOGGER.info("System RAM available " + mem.getRam());
         LOGGER.info("Information about the CPU " + cpu.toMap());
         LOGGER.info("Total memory free " + actualFree);
         LOGGER.info("Total memory used " + actualUsed);
         LOGGER.info("JVM free memory " + jvmFree);
         LOGGER.info("JVM total memory " + jvmTotal);

      } catch (SigarException e) {
         LOGGER.error("Error in getting system information from sigar..", e);
      } catch (IOException e) {
         LOGGER.error(
               "Error writing free memory % values to the memory log file", e);
      }
   }
}
