package de.tuda.sdm.dmdb.storage.types.exercise;

import de.tuda.sdm.dmdb.test.TestCase;
import org.junit.Assert;


public class TestSQLVarcharExtended extends TestCase {
	public void testSerializeDeserialize1(){
		String value = "123456789";

		/* test whether serialized length is VARIABLE length */
		SQLVarchar sqlVarchar = new SQLVarchar(value, 255);
		byte[] data = sqlVarchar.serialize();
		Assert.assertEquals(data.length, sqlVarchar.getVariableLength());

	}
}
