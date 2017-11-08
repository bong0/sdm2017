package de.tuda.sdm.dmdb.test.access;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestSuiteAccess extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "DMDB-Access" );
    suite.addTestSuite( TestHeapTable.class );
    return suite;
  }
}
