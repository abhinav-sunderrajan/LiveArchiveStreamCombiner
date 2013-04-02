package mcomp.dissertation.display;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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

@SuppressWarnings("serial")
public class GenericChartDisplay extends ApplicationFrame {

   protected TimeSeriesCollection dataset;
   private JFreeChart chart;
   private XYPlot plot;
   private ValueAxis axis;
   private ChartPanel chartPanel;
   private XYItemRenderer r;
   private Map<Integer, TimeSeries> timeSeriesMap;
   private String title;

   /**
    * 
    * @param timeseriesList
    */
   public GenericChartDisplay(String title) {
      super(title);
      this.title = title;
      dataset = new TimeSeriesCollection();
      timeSeriesMap = new HashMap<Integer, TimeSeries>();
      settings();

   }

   /**
    * Creates the settings for the time-series display can be overridden if the
    * sub class deems necessary.
    */
   protected void settings() {
      chart = ChartFactory.createTimeSeriesChart(title, "Time", "Value",
            dataset, true, true, false);
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
    * Override to refresh all the time series values
    * @param values
    */
   public synchronized void refreshDisplayValues(Map<Integer, Double> values) {
      Iterator<Entry<Integer, Double>> it = values.entrySet().iterator();
      while (it.hasNext()) {
         Map.Entry pairs = (Map.Entry) it.next();
         timeSeriesMap.get(pairs.getKey()).addOrUpdate(new Minute(),
               (Double) pairs.getValue());
      }
   }

   /**
    * 
    * @param series
    * @param key
    */
   public synchronized void addToDataSeries(TimeSeries series, int key) {
      dataset.addSeries(series);
      timeSeriesMap.put(key, series);
   }

}
