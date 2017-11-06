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
	public SQLInteger(){ super();
		this.setMaxLength(8); // we need more space due to attribute metadata
	}

	/**
	 * Constructor with value
	 * @param value Integer value
	 */
	public SQLInteger(int value){
		super(value);
		this.setMaxLength(8); // we need more space due to attribute metadata
	}

	private static short meta_offset = 2*Short.BYTES;// skip bytes from attribute metadata


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
		System.arraycopy(valueBytes,0, out, attrMetaBytes.length, valueBytes.length);

		return out;
	}

	@Override
	public void deserialize(byte[] data) {

		byte[] metabytes = new byte[2*Short.BYTES];
		System.arraycopy(
				data, 0,
				metabytes, 0,
				2*Short.BYTES
		);
		RowPage.AttributeMetadata meta = RowPage.extractAttributeMetadata(metabytes);

		if(meta.datatype != RowPage.DATATYPE_INT){
			throw new RuntimeException("Expected datatype in attribute metadata to be INT");
		} else if (meta.maxlen != 0){
			throw new RuntimeException("Expected unused maxlen attribute in metadata to be 0");
		}
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

	@Override
	public int getFixedLength() {
		return 8; //fixed length -- different from base class since we work with the attribute metadata
	}

}


/* TODO: write test that checks whether serialized length is fixed length */