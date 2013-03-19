package mcomp.dissertation.database.streamer.beans;

import java.sql.Timestamp;

/**
 * Bean class representing the LTA link data.
 */
public class LiveBean {

   private long linkId;
   private Timestamp timeStamp;
   private float avgSpeed;
   private float avgVolume;
   private String eventTime;

   /**
    * @return the avgVolume
    */
   public float getAvgVolume() {
      return avgVolume;
   }

   /**
    * @param avgVolume the avgVolume to set
    */
   public void setAvgVolume(final float avgVolume) {
      this.avgVolume = avgVolume;
   }

   /**
    * @return the avgSpeed
    */
   public float getAvgSpeed() {
      return avgSpeed;
   }

   /**
    * @param avgSpeed the avgSpeed to set
    */
   public void setAvgSpeed(final float avgSpeed) {
      this.avgSpeed = avgSpeed;
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
   public void setTimeStamp(final Timestamp timeStamp) {
      this.timeStamp = timeStamp;
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

   public String getEventTime() {
      return eventTime;
   }

   public void setEventTime(final String eventTime) {
      this.eventTime = eventTime;
   }

}
