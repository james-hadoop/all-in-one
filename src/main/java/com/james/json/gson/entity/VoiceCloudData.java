package com.james.json.gson.entity;

import java.io.Serializable;

public class VoiceCloudData implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String Mode;
	private String Token;
	private Device Device;
	private User User;
	private Data Data;

	public String getMode() {
		return Mode;
	}

	public void setMode(String mode) {
		Mode = mode;
	}

	public String getToken() {
		return Token;
	}

	public void setToken(String token) {
		Token = token;
	}

	public Device getDevice() {
		return Device;
	}

	public void setDevice(Device device) {
		this.Device = device;
	}

	public User getUser() {
		return User;
	}

	public void setUser(User user) {
		this.User = user;
	}

	public Data getData() {
		return Data;
	}

	public void setData(Data data) {
		this.Data = data;
	}

	public VoiceCloudData() {

	}
	
	public VoiceCloudData(boolean flag) {
		setMode("1");
		 setToken("abcdefg");
	}

	public String toString() {
		return "Mode: " + Mode + "\tToken: " + Token + "\tDevice: " + Device
				+ "\tUser: " + User + "\tData: " + Data;
	}
}
