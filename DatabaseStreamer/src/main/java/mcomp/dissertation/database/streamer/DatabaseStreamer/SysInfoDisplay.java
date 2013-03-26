package mcomp.dissertation.database.streamer.DatabaseStreamer;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.labels.StandardXYItemLabelGenerator;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.labels.XYItemLabelGenerator;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RectangleInsets;
import org.jfree.ui.RefineryUtilities;

@SuppressWarnings("deprecation")
/**
 * 
 * This is class is responsible for rendering the time series chart for monitoring the main system parameyets.
 * The monitored parameters are CPU usage, system memory and the JVM memory.
 *
 */
public class SysInfoDisplay extends ApplicationFrame {
   private TimeSeries totalMemFree = new TimeSeries("Total Free Memory %",
         Minute.class);
   private TimeSeries jvmMemoryFree = new TimeSeries("JVM Free Memory %",
         Minute.class);
   private TimeSeries cpuKernelTime = new TimeSeries("CPU kernel time/100000",
         Minute.class);
   private TimeSeriesCollection dataset;
   private JFreeChart chart;
   private XYPlot plot;
   private ValueAxis axis;
   private ChartPanel chartPanel;
   private XYItemRenderer r;

   /**
    * 
    * @param title
    */
   public SysInfoDisplay(String title) {
      super(title);
      dataset = new TimeSeriesCollection();
      dataset.addSeries(cpuKernelTime);
      dataset.addSeries(totalMemFree);
      dataset.addSeries(jvmMemoryFree);
      settings();

   }

   private void settings() {
      chart = ChartFactory.createTimeSeriesChart("System Parameters", "Time",
            "Value", dataset, true, true, false);
      chartPanel = new ChartPanel(chart);
      chartPanel.setPreferredSize(new java.awt.Dimension(1000, 500));
      chartPanel.setMouseZoomable(true, false);
      setContentPane(chartPanel);
      plot = chart.getXYPlot();
      plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
      plot.setDomainCrosshairVisible(true);
      plot.setRangeCrosshairVisible(true);

      r = plot.getRenderer();
      if (r instanceof XYLineAndShapeRenderer) {
         XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
         renderer.setBaseShapesVisible(true);
         renderer.setBaseShapesFilled(true);
         renderer.setDrawSeriesLineAsPath(true);
         final StandardXYToolTipGenerator g = new StandardXYToolTipGenerator(
               StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
               new SimpleDateFormat("hh:mm"), new DecimalFormat("0.00"));
         renderer.setToolTipGenerator(g);
      }

      // label the points
      NumberFormat format = NumberFormat.getNumberInstance();
      format.setMaximumFractionDigits(2);
      XYItemLabelGenerator generator = new StandardXYItemLabelGenerator(
            StandardXYItemLabelGenerator.DEFAULT_ITEM_LABEL_FORMAT, format,
            format);
      r.setBaseItemLabelGenerator(generator);
      r.setBaseItemLabelsVisible(true);

      axis = plot.getDomainAxis();
      axis.setAutoRange(true);
      axis.setFixedAutoRange(600000.0);
      this.pack();
      RefineryUtilities.centerFrameOnScreen(this);
      this.setVisible(true);
   }

   /**
    * 
    * @param memFreePercent
    * @param jvmFreePercent
    * @param cpuKernel
    */
   public void refreshDisplayValues(double memFreePercent,
         double jvmFreePercent, double cpuKernel) {
      totalMemFree.addOrUpdate(new Minute(), memFreePercent);
      jvmMemoryFree.addOrUpdate(new Minute(), jvmFreePercent);
      cpuKernelTime.addOrUpdate(new Minute(), cpuKernel);

   }
}
