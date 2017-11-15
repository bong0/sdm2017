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
		//TODO: implement this method
	}
	
	@Override
	public AbstractRecord next() {
		//TODO: implement this method
		return null;
	}
	
	@Override
	public void close() {
		//TODO: implement this method
	}
}
