package net.canadensys.processing.occurrence.model;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/**
 * Model to keep info about resources. Resource is about the source archive.
 * @author canadensys
 *
 */
@Entity
@Table(name = "resource_management")
//allocationSize=1 is a workaround to avoid improper behavior
//http://acodapella.blogspot.ca/2011/06/hibernate-annotation-postgresql.html
@SequenceGenerator(name = "resource_management_resource_id_seq", sequenceName = "resource_management_resource_id_seq", allocationSize=1)
public class ResourceModel {
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "resource_management_resource_id_seq")
	private Integer resource_id;
	private String name;
	private String key;
	private String archive_url;
	//update to dataset_shortname
	private String source_file_id;
	private Date last_updated;
	
	public Integer getResource_id() {
		return resource_id;
	}
	public void setResource_id(Integer resource_id) {
		this.resource_id = resource_id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getArchive_url() {
		return archive_url;
	}
	public void setArchive_url(String archive_url) {
		this.archive_url = archive_url;
	}
	public String getSource_file_id() {
		return source_file_id;
	}
	public void setSource_file_id(String source_file_id) {
		this.source_file_id = source_file_id;
	}
	public Date getLast_updated() {
		return last_updated;
	}
	public void setLast_updated(Date last_updated) {
		this.last_updated = last_updated;
	}
}
