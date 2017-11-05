package de.tuda.sdm.dmdb.test.storage;

import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;

import org.junit.Assert;
import de.tuda.sdm.dmdb.test.TestCase;


public class TestRecord extends TestCase{
	public void testCreate1(){
		AbstractRecord r = new Record(2);
		SQLInteger v1 = new SQLInteger(123456789);
		SQLVarchar v2 = new SQLVarchar("Hello111", 10);
		r.setValue(0, v1);
		r.setValue(1, v2);
		
		Assert.assertEquals(12, r.getFixedLength());
		Assert.assertEquals(((String)v2.getValue()).getBytes().length, r.getVariableLength());
	}
}
