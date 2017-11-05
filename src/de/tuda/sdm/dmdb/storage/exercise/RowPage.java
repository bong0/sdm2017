package de.tuda.sdm.dmdb.storage.exercise;

import de.tuda.sdm.dmdb.catalog.objects.Attribute;
import de.tuda.sdm.dmdb.storage.AbstractPage;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Iterator;

import static java.lang.Math.max;

public class RowPage extends AbstractPage {

	private static int SCHEMA_BYTE_COUNT = 8;
	/**
	 * Constructir for a row page with a given (fixed) slot size
	 * @param slotSize
	 */
	public RowPage(int slotSize) {
		super(slotSize);
	}

	/*private class SlotMetadata {
		public  int slotNo;
		public  int fixedLengthBytes;
		public  int varLengthBytes;
		public  int varBeginOffset;
		public SlotMetadata(int slotNo, int fixedLengthBytes, int varLengthBytes, int varBeginOffset){
			this.slotNo=slotNo;
			this.fixedLengthBytes=fixedLengthBytes;
			this.varLengthBytes=varLengthBytes;
			this.varBeginOffset=varBeginOffset;
		}
	}

	final short metaDataSize = Short.BYTES * 3; // metadata per slot

	private void setSlotMetadata(short slotNo, short fixedLengthBytes, short varLengthBytes, short varBeginOffset){
		// don't store number of used slots in header


		if(slotNo*(metaDataSize)+metaDataSize > this.PAGE_SIZE){
			throw new IllegalArgumentException("Cannot store metadata for slot, exceeding Metadata allocation");
		}

		byte[] metaData = ByteBuffer.allocate(metaDataSize)
				.putShort(fixedLengthBytes)
				.putShort(varLengthBytes)
				.putShort(varBeginOffset)
				.array();

		System.arraycopy(
				metaData, 0,
				this.data, SCHEMA_BYTE_COUNT+slotNo*(metaDataSize),
				Short.BYTES
		);
	}

	private SlotMetadata getSlotMetadata(int slotNo){
		byte[] metaBytes = new byte[metaDataSize];
		System.arraycopy(
				this.data, slotNo*(metaDataSize),
				metaBytes, 0,
				Short.BYTES
		);
		ByteBuffer bb = ByteBuffer.allocate(metaDataSize).put(metaBytes);
		short fixedLengthBytes = bb.getShort();
		short varLengthBytes = bb.getShort();
		short varBeginOffset = bb.getShort();


		return new SlotMetadata(
				slotNo,
				fixedLengthBytes,
				varLengthBytes,
				varBeginOffset
		);
	}*/

	public static short DATATYPE_VARCHAR = 0x00000044;
	public static short DATATYPE_INT = 0x00000022;

	@Override
	public void insert(int slotNumber, AbstractRecord record, boolean doInsert) {

		int baseOffset = slotNumber*this.slotSize;


		// check if insert
		if(slotNumber > ((this.offset)/this.slotSize) ){
			throw new IllegalArgumentException("Slotnumber is invalid: new slots must be created leaving no gap to the highest numbered slot");
		}
		if(slotNumber < 0){
			throw new IllegalArgumentException("Slot numbers may not be negative");
		}
		// checks whether record fixed and variable size fit into page
		if(!recordFitsIntoPage(record)){
			throw new IllegalArgumentException("Not enough space to insert record");
		}


		// we need to shift slots
		if(doInsert){
			// check if slot is occupied
			if(baseOffset <= this.offset){
				int bytesToMove = this.offset - baseOffset;
				System.arraycopy(
						this.data, baseOffset,
						this.data, baseOffset+slotSize,
						bytesToMove
				);
			}
		}


		ByteBuffer bb = ByteBuffer.allocate(record.getFixedLength());
		if(record.getFixedLength() != this.slotSize){
			throw new IllegalArgumentException("The record to store does not match the slot width");
		}

		for (AbstractSQLValue value:record.getValues()) {
			if(value.isFixedLength()) {
				bb.put(value.serialize()); // write int INCLUDING data type indicator

			} else {
				// store variable data
				int varDataStart = offsetEnd - value.getVariableLength();
				System.arraycopy(
					value.serialize(), 0,
					this.data, varDataStart,
					value.getVariableLength()
				);
				this.offsetEnd = varDataStart; // update offsetEnd for variable data

				// now write fixed data to attribute in slot
				bb.put(genAttributeMetaBytes(DATATYPE_VARCHAR, (short)value.getMaxLength())); // data type indicator for varchar
				bb.putInt(varDataStart); // pointer to variable data

			}
		}
		// write fixed part
		System.arraycopy(bb.array(), 0, this.data, baseOffset, bb.array().length);

		// update offset
		this.offset = max(this.offset, slotNumber*this.slotSize+slotSize);

		this.numRecords++;
	}

	@Override
	public int insert(AbstractRecord record){
		int newSlotNo = ((this.offset)/this.slotSize);
		this.insert(newSlotNo, record, true);
		System.out.println("Inserted at slotno "+Integer.valueOf(newSlotNo).toString());
		return newSlotNo;
	}
	
	@Override
	public void read(int slotNumber, AbstractRecord record) {
		// TODO catch out of bounds slot numbers
		byte[] slotBytes = new byte[slotSize];
		System.arraycopy(
				this.data, slotNumber*slotSize,
				slotBytes, 0,
				slotSize
		);
		ByteBuffer bb = ByteBuffer.allocate(slotSize);
		bb.put(slotBytes);
		bb.rewind();

		ArrayList<AbstractSQLValue> extValues=new ArrayList<AbstractSQLValue>();
		while(bb.hasRemaining()){
			byte[] metabytes = new byte[2*Short.BYTES];
			bb.get(metabytes);

			AttributeMetadata meta = extractAttributeMetadata(metabytes);

			if(meta.datatype == DATATYPE_INT){
				SQLInteger val = new SQLInteger();
				byte[] valuebytes = new byte[Integer.BYTES+metabytes.length];
				bb.position(bb.position()-metabytes.length); // rewind buffer 4 bytes to we re-ingest the metadata so deserialize of sqlint is happy
				bb.get(valuebytes);

				val.deserialize(valuebytes);
				extValues.add(val);

			} else if(meta.datatype == DATATYPE_VARCHAR){
				SQLVarchar val = new SQLVarchar(meta.maxlen);
				int varDataOffset = bb.getInt();

				byte[] stringbuf = new byte[meta.maxlen * 4]; // 4 bytes is the max len of bytes for one utf8 char
				System.arraycopy(
						this.data, varDataOffset,
						stringbuf, 0,
						stringbuf.length
				); // read out max string length in bytes from data buffer
				val.deserialize(stringbuf);
				extValues.add(val);
			} else {
				// don't throw an exception here since the overridden method does not either (and we may not modify it)
				throw new RuntimeException("Cannot identify datatype of attribute: "+Integer.toHexString(meta.datatype));
			}
		}

		Iterator<AbstractSQLValue> li = extValues.listIterator();
		for(int i=0; i<extValues.size(); i++){
			record.setValue(i, li.next()); // add actual attribute value to record to generate
		}
	}



	// maxlen is only used to store maximum length of string
	public static byte[] genAttributeMetaBytes(short datatype, short maxlen){
		ByteBuffer bb = ByteBuffer.allocate(2*Short.BYTES);
		bb.putShort(datatype);
		bb.putShort(maxlen);
		return bb.array();
	}

	private class AttributeMetadata {
		public short datatype;
		public short maxlen;
		public AttributeMetadata(short datatype, short maxlen){
			this.datatype=datatype;
			this.maxlen=maxlen;
		}
	}
	private AttributeMetadata extractAttributeMetadata(byte[] metabytes){
		ByteBuffer bb = ByteBuffer.allocate(2*Short.BYTES);
		bb.put(metabytes);
		bb.rewind();
		short datatype = bb.getShort();
		short maxlen = bb.getShort();
		return new AttributeMetadata(datatype, maxlen);
	}
}

