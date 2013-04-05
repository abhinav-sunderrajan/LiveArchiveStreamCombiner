package mcomp.dissertation.display;

@SuppressWarnings({ "serial" })
/**
 * 
 * To create a Singleton instance for checking parameters like latency and throughput of the application.
 *
 */
public class StreamJoinDisplay extends GenericChartDisplay {
   private static StreamJoinDisplay instance;

   /**
    * The title of the display
    * @param title
    */
   private StreamJoinDisplay(String title) {
      super(title);

   }

   /**
    * 
    * @param title
    * @returns a singleton instance
    */
   public static StreamJoinDisplay getInstance(String title) {
      if (instance == null) {
         instance = new StreamJoinDisplay(title);
      }
      return instance;

   }

}
