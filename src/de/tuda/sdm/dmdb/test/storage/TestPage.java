package de.tuda.sdm.dmdb.test.storage;

import org.junit.Assert;

import de.tuda.sdm.dmdb.storage.AbstractPage;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.exercise.RowPage;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.sdm.dmdb.test.TestCase;


public class TestPage extends TestCase{
	public void testInsertRecord(){
		//insert record
		AbstractRecord r1 = new Record(2);
		r1.setValue(0, new SQLInteger(123456789));
		r1.setValue(1, new SQLVarchar("Test", 10));
		AbstractPage p = new RowPage(r1.getFixedLength());
		p.insert(r1);
		
		//read record
		AbstractRecord r1cmp = new Record(2);
		r1cmp.setValue(0, new SQLInteger());
		r1cmp.setValue(1, new SQLVarchar(10));
		p.read(0, r1cmp);
		
		//compare
		Assert.assertEquals(r1, r1cmp);
	}
}
