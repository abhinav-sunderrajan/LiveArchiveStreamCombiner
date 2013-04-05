package mcomp.dissertation.beans;

import java.sql.Timestamp;

/**
 * Bean class for the traffic data records.
 */
public class HistoryBean {
   private int volume;
   private float speed;
   private long linkId;
   private int readingMinutes;
   private int readingSeconds;
   private int readingHours;

   /**
    * @param volume
    * @param speed
    * @param linkId
    * @param timeStamp
    */
   @SuppressWarnings("deprecation")
   public HistoryBean(final int volume, final float speed, final long linkId,
         final Timestamp timeStamp) {
      this.volume = volume;
      this.speed = speed;
      this.linkId = linkId;
      this.readingMinutes = timeStamp.getMinutes();
      this.readingSeconds = timeStamp.getSeconds();
      this.readingHours = timeStamp.getHours();
   }

   /**
    * @return the volume
    */
   public int getVolume() {
      return volume;
   }

   /**
    * @param volume the volume to set
    */
   public void setVolume(final int volume) {
      this.volume = volume;
   }

   /**
    * @return the speed
    */
   public float getSpeed() {
      return speed;
   }

   /**
    * @param speed the speed to set
    */
   public void setSpeed(final float speed) {
      this.speed = speed;
   }

   /**
    * @return the linkId
    */
   public long getLinkId() {
      return linkId;
   }

   /**
    * @param linkId the linkId to set
    */
   public void setLinkId(final long linkId) {
      this.linkId = linkId;
   }

   public int getReadingMinutes() {
      return readingMinutes;
   }

   public void setReadingMinutes(final int readingMinutes) {
      this.readingMinutes = readingMinutes;
   }

   public int getReadingSeconds() {
      return readingSeconds;
   }

   public void setReadingSeconds(final int readingSeconds) {
      this.readingSeconds = readingSeconds;
   }

   public int getReadingHours() {
      return readingHours;
   }

   public void setReadingHours(final int readingHours) {
      this.readingHours = readingHours;
   }

}
