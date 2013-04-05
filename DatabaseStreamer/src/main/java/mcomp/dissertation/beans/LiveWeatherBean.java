package mcomp.dissertation.beans;

import java.sql.Timestamp;

public class LiveWeatherBean {

   private long linkId;
   private double rain;
   private double temperature;
   private Timestamp timeStamp;

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
    * @return the rain
    */
   public double getRain() {
      return rain;
   }

   /**
    * @param rain the rain to set
    */
   public void setRain(double rain) {
      this.rain = rain;
   }

   /**
    * @return the temperature
    */
   public double getTemperature() {
      return temperature;
   }

   /**
    * @param temperature the temperature to set
    */
   public void setTemperature(double temperature) {
      this.temperature = temperature;
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
