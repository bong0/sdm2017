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
	public static int LENGTH = 8;  // 4B attr metadata, 4B address pointer
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

	public static final int headerLength = Short.BYTES; // header carries string length

	private static short meta_offset = 2*Short.BYTES;// bytes from attribute metadata

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
		byte[] buf = new byte[valueUtf8Bytes.length + headerLength];

		// form short integer of payload (string) length in bytes to use as header
		byte[] header = new byte[]{
				(byte) ((valueUtf8Bytes.length << 16) >>> 24), // length
				(byte) ((valueUtf8Bytes.length << 24) >>> 24) // length
		};


		// put header into array
		System.arraycopy(
				header, 0,
				buf, 0,
				headerLength
		);

		// push string into array
		System.arraycopy(
				valueUtf8Bytes, 0,
				buf, headerLength,
				valueUtf8Bytes.length
		);


		return buf;
	}


	@Override
	public void deserialize(byte[] data) {

		byte[] header = new byte[headerLength];
		System.arraycopy(data, 0, header, 0, headerLength);
		short payloadLen = (short)((header[0] & 0xFF) << 8 | (header[1] & 0xFF));

		byte[] payload = new byte[payloadLen];
		System.arraycopy(
				data, headerLength,
				payload, 0,
				payloadLen
		);
		this.value = new String(payload, StandardCharsets.UTF_8);
	}
	
	@Override
	public SQLVarchar clone(){
		return new SQLVarchar(this.value, this.maxLength);
	}

}