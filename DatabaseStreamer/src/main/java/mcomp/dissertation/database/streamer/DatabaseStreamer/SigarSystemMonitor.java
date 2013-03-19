package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.util.Map;

import org.apache.log4j.Logger;
import org.hyperic.sigar.Cpu;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * 
 * This thread will be responsible for monitoring and reporting the system
 * parameters at a regular interval. it makes use of the Sigar api.
 * 
 */

public class SigarSystemMonitor implements Runnable {
   private CpuInfo[] cpuinfo;
   private Cpu cpu;
   private Mem mem;
   private Sigar sigar;
   private static SigarSystemMonitor instance;
   private static final Logger LOGGER = Logger
         .getLogger(SigarSystemMonitor.class);

   /**
    * private constructor singleton pattern
    */
   private SigarSystemMonitor() {
      sigar = new Sigar();
      // sigar.enableLogging(true);
      try {
         cpuinfo = sigar.getCpuInfoList();
         cpu = sigar.getCpu();
         mem = sigar.getMem();
         for (int i = 0; i < cpuinfo.length; i++) {
            Map map = cpuinfo[i].toMap();
            LOGGER.info("CPU " + i + ": " + map);
         }
         LOGGER.info("System RAM available " + mem.getRam());

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

   public void run() {

      LOGGER.info("Information about the CPU " + cpu.toMap());
      LOGGER.info("Total memory free " + mem.getActualFree());
      LOGGER.info("Total memory used " + mem.getActualUsed());
   }
}
