package de.tuda.sdm.dmdb.catalog.objects;

import de.tuda.sdm.dmdb.storage.types.SQLType;

public class DataType {
	private SQLType type;
	private int length=0;
	
	public SQLType getType() {
		return type;
	}
	public void setType(SQLType type) {
		this.type = type;
	}
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
}

