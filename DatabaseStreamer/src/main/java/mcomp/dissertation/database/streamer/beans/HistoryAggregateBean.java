package mcomp.dissertation.database.streamer.beans;

/**
 * The bean class representing the aggregated stream of all the archived
 * sub-streams.
 */
public class HistoryAggregateBean {

   private double aggregateSpeed;
   private double aggregateVolume;
   private long linkId;
   private int hrs;
   private int mins;

   /**
    * @return the aggregateSpeed
    */
   public double getAggregateSpeed() {
      return aggregateSpeed;
   }

   /**
    * @param aggregateSpeed the aggregateSpeed to set
    */
   public void setAggregateSpeed(final double aggregateSpeed) {
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
   public void setAggregateVolume(final double aggregateVolume) {
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
   public void setLinkId(final long linkId) {
      this.linkId = linkId;
   }

   public int getHrs() {
      return hrs;
   }

   public void setHrs(final int hrs) {
      this.hrs = hrs;
   }

   public int getMins() {
      return mins;
   }

   public void setMins(final int mins) {
      this.mins = mins;
   }

}
