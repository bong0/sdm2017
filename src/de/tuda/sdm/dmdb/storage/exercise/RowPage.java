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

	//private static int SCHEMA_BYTE_COUNT = 8;

	/**
	 * Constructor for a row page with a given (fixed) slot size
	 * @param slotSize
	 */
	public RowPage(int slotSize) {
		super(slotSize);
	}


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
		if(record.getFixedLength() != this.slotSize){
			throw new IllegalArgumentException("The record to store does not match the slot width");
		}


		// we need to shift slots
		if(doInsert){
			// check if slot is occupied
			if(baseOffset < this.offset){
				int bytesToMove = this.offset - baseOffset;
				System.arraycopy(
						this.data, baseOffset,
						this.data, baseOffset+slotSize,
						bytesToMove
				);
			}
			this.offset += slotSize; // advance offset since we needed to make space for slot on lower address
		}



		ByteBuffer bb = ByteBuffer.allocate(record.getFixedLength());

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
				//bb.put(genAttributeMetaBytes(DATATYPE_VARCHAR, (short)value.getMaxLength())); // data type indicator for varchar
				bb.putInt(varDataStart); // pointer to variable data
				bb.putInt(value.getVariableLength()); // set varlength in bytes
			}
		}

		// write fixed part
		System.arraycopy(bb.array(), 0, this.data, baseOffset, bb.array().length);

		// update offset
		this.offset = max(this.offset, slotNumber*this.slotSize+slotSize);

		if(doInsert) {
			this.numRecords++;
		}
	}

	@Override
	public int insert(AbstractRecord record){
		int newSlotNo = ((this.offset)/this.slotSize);
		this.insert(newSlotNo, record, true);
		return newSlotNo;
	}
	
	@Override
	public void read(int slotNumber, AbstractRecord record) {
		if(slotNumber < 0){
			throw new IllegalArgumentException("Slotno is invalid, must be 0 or positive");
		}
		if(slotNumber > ((this.offset-this.slotSize)/this.slotSize)){
			throw new IllegalArgumentException("Slotno is too large, not within the range of assigned slots");
		}

		byte[] slotBytes = new byte[this.slotSize];
		System.arraycopy(
				this.data, slotNumber*this.slotSize,
				slotBytes, 0,
				slotSize
		);
		ByteBuffer bb = ByteBuffer.allocate(this.slotSize);
		bb.put(slotBytes);
		bb.rewind();


		int currentRecord = 0;
		for(AbstractSQLValue valPrototype : record.getValues()){

			if(!bb.hasRemaining()){
				throw new RuntimeException("No bytes left to parse but still expected attributes to be available");
			}

			// read fixed length bytes
			byte[] payload = new byte[valPrototype.getFixedLength()];

			// we need to dereference/fetch the variable data
			if(valPrototype.isFixedLength()) {
				bb.get(payload);
				// decode fixed bytes and assign them to record to fill
				record.getValue(currentRecord).deserialize(payload);
				currentRecord++;
			} else {

				int varPointer = bb.getInt(); // begin of variable data
				int strLengthBytes = bb.getInt();

				System.out.println(varPointer);
				System.out.println(strLengthBytes);
				byte[] stringbuf = new byte[strLengthBytes];
				System.arraycopy(
						this.data, varPointer,
						stringbuf, 0,
						strLengthBytes
				); // read out max string length in bytes from data buffer
				record.getValue(currentRecord).deserialize(stringbuf);
				currentRecord++;
			}

		}

	}



	/*// maxlen is only used to store maximum length of string
	public static byte[] genAttributeMetaBytes(short datatype, short maxlen){
		ByteBuffer bb = ByteBuffer.allocate(2*Short.BYTES);
		bb.putShort(datatype);
		bb.putShort(maxlen);
		return bb.array();
	}

	public static class AttributeMetadata {
		public short datatype;
		public short maxlen;
		public AttributeMetadata(short datatype, short maxlen){
			this.datatype=datatype;
			this.maxlen=maxlen;
		}
	}
	public static AttributeMetadata extractAttributeMetadata(byte[] metabytes){
		ByteBuffer bb = ByteBuffer.allocate(2*Short.BYTES);
		bb.put(metabytes);
		bb.rewind();
		short datatype = bb.getShort();
		short maxlen = bb.getShort();
		return new AttributeMetadata(datatype, maxlen);
	}*/
}

