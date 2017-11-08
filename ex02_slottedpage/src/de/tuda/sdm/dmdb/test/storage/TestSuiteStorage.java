package de.tuda.sdm.dmdb.test.storage;

import de.tuda.sdm.dmdb.test.storage.types.TestSQLInteger;
import de.tuda.sdm.dmdb.test.storage.types.TestSQLVarchar;
import junit.framework.Test;
import junit.framework.TestSuite;

public class TestSuiteStorage extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "DMDB-Storage" );
    suite.addTestSuite( TestPage.class );
    suite.addTestSuite( TestRecord.class );
    suite.addTestSuite( TestSQLInteger.class );
    suite.addTestSuite( TestSQLVarchar.class );
    return suite;
  }
}
