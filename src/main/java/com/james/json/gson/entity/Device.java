package com.james.json.gson.entity;

import java.io.Serializable;

public class Device  implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 760348296450351668L;
	private String serial;
	private String mac;
	private String pos;
	private String type;

	public String getSerial() {
		return serial;
	}

	public void setSerial(String serial) {
		this.serial = serial;
	}

	public String getMac() {
		return mac;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public String getPos() {
		return pos;
	}

	public void setPos(String pos) {
		this.pos = pos;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Device() {

	}
	
	public Device(boolean flag){
		setSerial("James-001");
		setMac("01-02-03-04-05");
		setPos("Shanghai");
		setType("Basic001");
	}

	public String toString() {
		return "serial: " + serial + "\tmac: " + mac + "\tpos: " + pos
				+ "\ttype: " + type;
	}
}
