package de.tuda.sdm.dmdb.test.mapReduce;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestSuiteMapReduce extends TestSuite
{
  public static Test suite()
  {
	  TestSuite suite = new TestSuite( "DMDB-MapReduce" );
	    suite.addTestSuite(TestSinglePhaseMapReduce.class  );
	    suite.addTestSuite(TestMultiPhaseMapReduce.class  );
    return suite;
  }
}
