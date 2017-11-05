package de.tuda.sdm.dmdb.storage.types.exercise;

import de.tuda.sdm.dmdb.storage.exercise.RowPage;
import de.tuda.sdm.dmdb.storage.types.SQLIntegerBase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * SQL integer value
 * @author cbinnig
 *
 */
public class SQLInteger extends SQLIntegerBase {
	
	/**
	 * Constructor with default value
	 */
	public SQLInteger(){
		super();
	}
	
	/**
	 * Constructor with value
	 * @param value Integer value
	 */
	public SQLInteger(int value){
		super(value);
	}

	private static short meta_offset = 2*Short.BYTES;// skip bytes from attribute metadata

	@Override
	public int getFixedLength() {
		return LENGTH+meta_offset;
	}

	@Override
	public byte[] serialize() {
		byte[] attrMetaBytes = RowPage.genAttributeMetaBytes(RowPage.DATATYPE_INT, (short)0);
		byte[] valueBytes = new byte[] {
				// use >>> operator for right shifting so zeroes are carried in from left
				(byte) (this.value >>> 24), // lowest mem addr => MSB
				(byte) ((this.value << 8) >>> 24), // cut off MSB, then shift right to lowest position
				(byte) ((this.value << 16) >>> 24), // cut off MSB+second signif. byte
				(byte) ((this.value << 24) >>> 24) // highest mem addr => LSB
		};
		// concatenate the two arrays
		byte[] out = new byte[attrMetaBytes.length+valueBytes.length];
		System.arraycopy(attrMetaBytes, 0, out, 0, attrMetaBytes.length);
		System.arraycopy(valueBytes, 0, out, attrMetaBytes.length, valueBytes.length);

		return out;
	}

	@Override
	public void deserialize(byte[] data) {
		// TODO: implement check that indicator is correct
		    // interpret bytes as unsigned through masking with AND 0xFF to get lower part
			this.value =(data[meta_offset+0] & 0xFF) << 24 |
				(data[meta_offset+1]& 0xFF) << 16 |
				(data[meta_offset+2]& 0xFF) << 8 |
				(data[meta_offset+3]& 0xFF);
	}

	@Override
	public SQLInteger clone(){
		return new SQLInteger(this.value);
	}

}


/* TODO: write test that checks whether serialized length is fixed length */