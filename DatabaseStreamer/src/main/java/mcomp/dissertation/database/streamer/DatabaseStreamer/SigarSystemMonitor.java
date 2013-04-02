package mcomp.dissertation.database.streamer.DatabaseStreamer;

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

   /**
    * private constructor singleton pattern
    */

   private SigarSystemMonitor() {
      sigar = new Sigar();
      // Settings for display. The code is ugly need to figure out a better way
      // of doing things here.
      display = new GenericChartDisplay("System Parameters");
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
    * @return SigarSystemMonitor
    */

   public static SigarSystemMonitor getInstance() {
      if (instance == null) {
         instance = new SigarSystemMonitor();
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
      } catch (SigarException e) {
         LOGGER.error("Error in getting system information from sigar..", e);
      }
      long actualFree = mem.getActualFree();
      long actualUsed = mem.getActualUsed();
      long jvmFree = Runtime.getRuntime().freeMemory();
      long jvmTotal = Runtime.getRuntime().totalMemory();
      // Update display values and send to chart
      valueMap.put(1, mem.getFreePercent());
      valueMap.put(2, ((jvmFree * 100.0) / jvmTotal));
      valueMap.put(3, cpu.getSys() / cpuUsageScalefactor);
      display.refreshDisplayValues(valueMap);

      LOGGER.info("System RAM available " + mem.getRam());
      LOGGER.info("Information about the CPU " + cpu.toMap());
      LOGGER.info("Total memory free " + actualFree);
      LOGGER.info("Total memory used " + actualUsed);
      LOGGER.info("JVM free memory " + jvmFree);
      LOGGER.info("JVM total memory " + jvmTotal);
   }
}
