package de.tuda.sdm.dmdb.test.sql;

import org.junit.Assert;
import de.tuda.sdm.dmdb.test.TestCase;
import de.tuda.sdm.dmdb.sql.SQLCompiler;
import de.tuda.sdm.dmdb.sql.statement.SelectStatement;
import de.tuda.sdm.dmdb.sql.statement.Statement;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.sql.error.Error;


public class TestSql extends TestCase{
	private Statement compile(String sql){
		SQLCompiler compiler = new SQLCompiler();
		Statement stmt = compiler.compile(sql);
		
		if(compiler.getLastError().isError()){
			Assert.assertTrue("Execute failed: "+compiler.getLastError(), false);
		}
		
		return stmt;
	}
	
	private void execute(Statement stmt){
		Error lastError = stmt.execute();
		
		if(lastError.isError()){
			Assert.assertTrue("Execute failed: "+lastError, false);
		}
	}
		
	public void testSimpleSelect(){
		//CREATE TABLE statements and INSERT statements
		Statement stmt = this.compile("CREATE TABLE t4 ( a int, b int )");
		this.execute(stmt);
		
		stmt = this.compile("INSERT INTO t4 VALUES (1,1)");
		this.execute(stmt);
		stmt = this.compile("INSERT INTO t4 VALUES (2,1)");
		this.execute(stmt);
		stmt = this.compile("INSERT INTO t4 VALUES (3,2)");
		this.execute(stmt);
		
		stmt = this.compile("CREATE TABLE t5 ( c int, d int )");
		this.execute(stmt);
		
		stmt = this.compile("INSERT INTO t5 VALUES (1,1)");
		this.execute(stmt);
		stmt = this.compile("INSERT INTO t5 VALUES (1,2)");
		this.execute(stmt);
		stmt = this.compile("INSERT INTO t5 VALUES (2,2)");
		this.execute(stmt);
		
		//first SELECT statement
		SelectStatement selectStmt = (SelectStatement)this.compile("SELECT a, c FROM t4, t5 WHERE b=c AND d=2");
		this.execute(selectStmt);
		
		for(AbstractRecord rec: selectStmt.getResultSet()){
			System.out.println(rec);
		}
		
		Assert.assertEquals(3, selectStmt.getResultSet().size());
		
		//second SELECT statement
		selectStmt = (SelectStatement)this.compile("SELECT a,b FROM t4 WHERE a=2");
		this.execute(selectStmt);
		
		for(AbstractRecord rec: selectStmt.getResultSet()){
			System.out.println(rec);
		}
		
		Assert.assertEquals(1, selectStmt.getResultSet().size());
	}
}
