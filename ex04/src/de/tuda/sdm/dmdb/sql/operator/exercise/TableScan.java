package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.util.Iterator;

import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.TableScanBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

@SuppressWarnings("unused")
public class TableScan extends TableScanBase {
	
	public TableScan(AbstractTable table){
		super(table);
	}

	@Override
	public void open() {
		this.tableIter = this.table.iterator();
	}
	
	@Override
	public AbstractRecord next() {

		while(this.tableIter != null && this.tableIter.hasNext()){
			AbstractRecord tmp = this.tableIter.next();
			return tmp.clone();
		}
		return null;
	}
	
	@Override
	public void close() {

		this.tableIter=null;
	}
}
