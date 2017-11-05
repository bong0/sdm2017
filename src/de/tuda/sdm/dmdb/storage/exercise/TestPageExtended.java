package de.tuda.sdm.dmdb.storage.exercise;

import de.tuda.sdm.dmdb.storage.AbstractPage;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.sdm.dmdb.test.TestCase;
import org.junit.Assert;


public class TestPageExtended extends TestCase{
	public void testInsertRecord(){
		//insert record
		AbstractRecord r1 = new Record(2);
		r1.setValue(0, new SQLInteger(123456789));
		r1.setValue(1, new SQLVarchar("Foobar", 10));
		AbstractPage p = new RowPage(r1.getFixedLength());
		int slot = p.insert(r1);


		/*
		insert unaligned into free space
		 */
		boolean thrown=false;
		try {
			p.insert(slot + 2, r1, true);
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertEquals(thrown, true);

		/*
		 insert unaligned into free space (update, not insert)
		 */
		thrown=false;
		try {
			p.insert(slot + 2, r1, false);
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertEquals(thrown, true);

		/*
		 insert with invalid page number
		 */
		thrown=false;
		try {
			p.insert(-1, r1, true);
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertEquals(thrown, true);

		int minRecordsFit = p.estimateRecords(r1); // get minimum number of records that should fit without problem
		for(int i=0; i<minRecordsFit; i++){
			p.insert(r1);
		}
		thrown=false;
		int thrcount = 0;
		try {
			while(true){
				thrcount++;
				p.insert(r1);
			}
		} catch (IllegalArgumentException e) {
			thrown = true;
			System.out.println("thrown after iterations: "+thrcount);
		}
		Assert.assertEquals(thrown, true);


	}
}
