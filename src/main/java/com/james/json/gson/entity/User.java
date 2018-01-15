package com.james.json.gson.entity;

import java.io.Serializable;

public class User  implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4581944873892946701L;
	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public User() {

	}
	
	public User(boolean flag){
		setName("James");
	}

	public String toString() {
		return "name: " + name;
	}
}
