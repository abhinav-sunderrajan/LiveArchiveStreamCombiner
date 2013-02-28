package mcomp.dissertation.database.streamer.beans;

import java.sql.Timestamp;

/**
 * The bean class representing the aggregated stream of all the archived
 * sub-streams.
 * 
 */
public class HistoryAggregateBean {

   private double aggregateSpeed;
   private double aggregateVolume;
   private long linkId;
   private Timestamp timeStamp;

   /**
    * @return the aggregateSpeed
    */
   public double getAggregateSpeed() {
      return aggregateSpeed;
   }

   /**
    * @param aggregateSpeed the aggregateSpeed to set
    */
   public void setAggregateSpeed(double aggregateSpeed) {
      this.aggregateSpeed = aggregateSpeed;
   }

   /**
    * @return the aggregateVolume
    */
   public double getAggregateVolume() {
      return aggregateVolume;
   }

   /**
    * @param aggregateVolume the aggregateVolume to set
    */
   public void setAggregateVolume(double aggregateVolume) {
      this.aggregateVolume = aggregateVolume;
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

   public Timestamp getTimeStamp() {
      return timeStamp;
   }

   public void setTimeStamp(Timestamp timeStamp) {
      this.timeStamp = timeStamp;
   }

}
