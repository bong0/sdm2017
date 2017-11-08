package de.tuda.sdm.dmdb.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import de.tuda.sdm.dmdb.test.access.TestSuiteAccess;
import de.tuda.sdm.dmdb.test.storage.TestSuiteStorage;

public class TestSuiteDMDB extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "DMDB-All" );
    suite.addTest(TestSuiteStorage.suite());
    suite.addTest(TestSuiteAccess.suite());
    return suite;
  }
}
