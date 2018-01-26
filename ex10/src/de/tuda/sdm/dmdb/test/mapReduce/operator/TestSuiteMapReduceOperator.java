package de.tuda.sdm.dmdb.test.mapReduce.operator;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestSuiteMapReduceOperator extends TestSuite
{
  public static Test suite()
  {
	  TestSuite suite = new TestSuite( "DMDB-MapReduceOperator" );
	    suite.addTestSuite(TestMapper.class  );
	    suite.addTestSuite(TestReducer.class  );
    return suite;
  }
}
