package de.tuda.sdm.dmdb.test.storage.types;

import org.junit.Assert;

import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.sdm.dmdb.test.TestCase;


public class TestSQLVarchar  extends TestCase {
	public void testSerializeDeserialize1(){
		String value = "123456789";
		
		SQLVarchar sqlVarchar = new SQLVarchar(value, 255);
		byte[] data = sqlVarchar.serialize();
		
		SQLVarchar sqlVarchar2 = new SQLVarchar(255);
		sqlVarchar2.deserialize(data);
		
		Assert.assertEquals(sqlVarchar.getValue(), sqlVarchar2.getValue());
	}
}
