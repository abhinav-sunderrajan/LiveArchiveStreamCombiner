package mcomp.dissertation.database.streamer.beans;

import java.sql.Timestamp;

/**
 * Bean class for the traffic data records.
 * 
 */
public class HistoryBean {
   private int volume;
   private float speed;
   private long linkId;
   private Timestamp timeStamp;

   /**
    * @param volume
    * @param speed
    * @param linkId
    * @param timeStamp
    */
   public HistoryBean(int volume, float speed, long linkId, Timestamp timeStamp) {
      this.volume = volume;
      this.speed = speed;
      this.linkId = linkId;
      this.timeStamp = timeStamp;

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
   public void setVolume(int volume) {
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
   public void setSpeed(float speed) {
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
   public void setLinkId(long linkId) {
      this.linkId = linkId;
   }

   /**
    * @return the timeStamp
    */
   public Timestamp getTimeStamp() {
      return timeStamp;
   }

   /**
    * @param timeStamp the timeStamp to set
    */
   public void setTimeStamp(Timestamp timeStamp) {
      this.timeStamp = timeStamp;
   }

}
