package de.tuda.sdm.dmdb.storage.types.exercise;

import de.tuda.sdm.dmdb.storage.exercise.RowPage;
import de.tuda.sdm.dmdb.storage.types.SQLVarcharBase;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * SQL varchar value
 * @author cbinnig
 *
 */
public class SQLVarchar extends SQLVarcharBase {	
	/**
	 * Constructor with default value and max. length 
	 * @param maxLength
	 */

	public SQLVarchar(int maxLength){
		super(maxLength);
	}
	
	/**
	 * Constructor with string value and max. length 
	 * @param value
	 * @param maxLength
	 */
	public SQLVarchar(String value, int maxLength){
		super(value, maxLength);
	}

	@Override
	public byte[] serialize() {
		byte[] valueUtf8Bytes = null;
		try {
			valueUtf8Bytes = value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e){
			e.printStackTrace();
			return null;
		}

		// just variable data
		byte[] buf = new byte[valueUtf8Bytes.length];

		// push string into array
		System.arraycopy(
				valueUtf8Bytes, 0,
				buf, 0,
				valueUtf8Bytes.length
		);


		return buf;
	}


	@Override
	public void deserialize(byte[] data) {
		if(data == null){
			throw new IllegalArgumentException("Cannot deserialize null value");
		}
		this.value = new String(data, StandardCharsets.UTF_8);
	}
	
	@Override
	public SQLVarchar clone(){
		return new SQLVarchar(this.value, this.maxLength);
	}

}