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
    * @param imageSaveDirectory
    */
   private StreamJoinDisplay(final String title, final String imageSaveDirectory) {
      super(title, imageSaveDirectory);

   }

   /**
    * 
    * @param title
    * @param imageSaveDirectory
    * @returns a singleton instance
    */
   public static StreamJoinDisplay getInstance(String title,
         String imageSaveDirectory) {
      if (instance == null) {
         instance = new StreamJoinDisplay(title, imageSaveDirectory);
      }
      return instance;

   }

}
