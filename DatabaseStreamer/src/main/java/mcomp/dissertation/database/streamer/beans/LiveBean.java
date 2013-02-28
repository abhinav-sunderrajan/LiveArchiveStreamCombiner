package mcomp.dissertation.database.streamer.beans;

import java.sql.Timestamp;

/**
 * Bean class representing the LTA link data.
 * 
 */
public class LiveBean {

	private int linkId;
	private Timestamp timeStamp;
	private float avgSpeed;
	private float avgVolume;

	/**
	 * @return the avgVolume
	 */
	public float getAvgVolume() {
		return avgVolume;
	}

	/**
	 * @param avgVolume
	 *            the avgVolume to set
	 */
	public void setAvgVolume(float avgVolume) {
		this.avgVolume = avgVolume;
	}

	/**
	 * @return the avgSpeed
	 */
	public float getAvgSpeed() {
		return avgSpeed;
	}

	/**
	 * @param avgSpeed
	 *            the avgSpeed to set
	 */
	public void setAvgSpeed(float avgSpeed) {
		this.avgSpeed = avgSpeed;
	}

	/**
	 * @return the timeStamp
	 */
	public Timestamp getTimeStamp() {
		return timeStamp;
	}

	/**
	 * @param timeStamp
	 *            the timeStamp to set
	 */
	public void setTimeStamp(Timestamp timeStamp) {
		this.timeStamp = timeStamp;
	}

	/**
	 * @return the linkId
	 */
	public int getLinkId() {
		return linkId;
	}

	/**
	 * @param linkId
	 *            the linkId to set
	 */
	public void setLinkId(int linkId) {
		this.linkId = linkId;
	}

}
