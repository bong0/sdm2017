package de.tuda.sdm.dmdb.test.sql;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestSuiteSQL extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "DMDB-SQL" );
    suite.addTestSuite( TestSort.class );
    return suite;
  }
}
