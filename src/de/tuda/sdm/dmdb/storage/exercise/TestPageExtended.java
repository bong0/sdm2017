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

		Assert.assertEquals(0, slot);

		/*
		Test if updating a slot works correctly
		 */
		int numrec = p.getNumRecords();
		AbstractRecord r2 = new Record(2);
		r2.setValue(0, new SQLInteger(987654321));
		r2.setValue(1, new SQLVarchar("Updated", 10));
		p.insert(0, r2.clone(), false); // insert slot at same slotno that already exists
		AbstractRecord readRec = new Record(2); // create record that is capable of storing 2 attrs
		readRec.setValue(0, new SQLInteger());
		readRec.setValue(1, new SQLVarchar(10));

		p.read(0, readRec); // read record
		// verify if content is the same as we wrote
		Assert.assertEquals(readRec, r2);
		// verify that number of records did NOT change
		Assert.assertEquals(numrec, p.getNumRecords());
		numrec = -10000; // invalidate value


		/*
		Verify second insert at same slotno results shifting
		 */
		boolean thrown=false;
		try {
			int numrecs_beforeshift = p.getNumRecords();
			p.insert(0, r1.clone(), true); // insert slot at same slotno that already exists
			Assert.assertEquals(p.getNumRecords(),numrecs_beforeshift+1); // one record should have been created
			slot++; // count new record
			p.read(1, readRec);
			Assert.assertEquals(readRec, r2); // the record which was written last before in the previous test should now be the last one after the shift
		} catch (Exception e){
			thrown=true;
			e.printStackTrace();
		}
		Assert.assertEquals(false, thrown);


		/*
		insert unaligned into free space
		 */
		thrown=false;
		try {
			p.insert(slot + 30, r1.clone(), true);
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertEquals(true, thrown);

		/*
		 insert unaligned into free space (update, not insert)
		 */
		thrown=false;
		try {
			p.insert(slot + 30, r1.clone(), false);
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertEquals(true, thrown);

		/*
		test wether reading a non-existent slot throws an exception
		 */
		thrown=false;
		try {
			p.read(slot + 40, readRec);
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertEquals(true, thrown);

		/*
		 insert with invalid page number
		 */
		thrown=false;
		try {
			p.insert(-1, r1, true);
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertEquals(true, thrown);

		/*
		Test that page fits at least calculated records and after that probe for failure
		 */
		int minRecordsFit = p.estimateRecords(r1); // get minimum number of records that should fit without problem
		System.out.println(minRecordsFit+" should fit into page");
		for(int i=0; i<minRecordsFit; i++){
			p.insert(r1);
			++slot; // increment slots that should have been allocated
		}
		thrown=false;
		int thrcount = 0;
		try {
			while(true){
				thrcount++;
				int followslot = -1;
				followslot = p.insert(r1.clone());
				Assert.assertEquals(followslot, ++slot); // calculated slotno should match returned one
			}
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertEquals(thrown, true);


	}
}
