package com.james.demo.json.gson.entity;

import java.io.Serializable;

public class Data implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3780364058196198231L;
	private int idx;
	private int iflag;
	private int ilen;
	private String entity;
	
	

	public int getIdx() {
		return idx;
	}

	public void setIdx(int idx) {
		this.idx = idx;
	}

	public int getIflag() {
		return iflag;
	}

	public void setIflag(int iflag) {
		this.iflag = iflag;
	}

	public int getIlen() {
		return ilen;
	}

	public void setIlen(int ilen) {
		this.ilen = ilen;
	}

	public String getEntity() {
		return entity;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}

	public Data() {

	}
	
	public Data(boolean flag){
		setIdx(1);
		setIflag(0);
		setIlen(11);
		setEntity("hello james");
	}

	public String toString() {
		return "idx: " + idx + "\tiflag: " + iflag + "\tilen: " + ilen
				+ "\tentity: " + entity;
	}
}
