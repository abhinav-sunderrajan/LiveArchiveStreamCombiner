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

   /**
    * @return the hrs
    */

   public int getHrs() {
      return hrs;
   }

   /**
    * @param hrs the hrs to set
    */

   public void setHrs(final int hrs) {
      this.hrs = hrs;
   }

   /**
    * @return the mins
    */
   public int getMins() {
      return mins;
   }

   /**
    * @param mins the mins to set
    */
   public void setMins(final int mins) {
      this.mins = mins;
   }

   @Override
   public String toString() {

      return "Speed: " + aggregateSpeed + " Volume: " + aggregateVolume
            + " at time" + hrs + ":" + mins + " on link " + linkId;

   }

}
