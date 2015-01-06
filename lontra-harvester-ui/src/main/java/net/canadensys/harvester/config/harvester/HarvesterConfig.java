package net.canadensys.harvester.config.harvester;

/**
 * Havester configuration class
 * 
 * @author canadensys
 * 
 */
public class HarvesterConfig implements HarvesterConfigIF {

	private String iptRssAddress;

	public String getIptRssAddress() {
		return iptRssAddress;
	}

	public void setIptRssAddress(String iptRssAddress) {
		this.iptRssAddress = iptRssAddress;
	}

}
