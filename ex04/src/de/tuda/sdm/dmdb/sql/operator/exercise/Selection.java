package de.tuda.sdm.dmdb.sql.operator.exercise;

import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.SelectionBase;
import de.tuda.sdm.dmdb.sql.operator.UnaryOperator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

@SuppressWarnings("unused")
public class Selection extends SelectionBase {
	
	public Selection(Operator child, int attribute, AbstractSQLValue constant) {
		super(child, attribute, constant);
	}

	@Override
	public void open() {
		child.open();
	}
	
	@Override
	public AbstractRecord next() {

		AbstractRecord tmpRec=null;
		while((tmpRec = child.next()) != null){
			if(tmpRec.getValue(attribute).equals(constant)){
				return tmpRec.clone();
			} else {
			}
		}
		return null;
	}
	
	@Override
	public void close() {
		child.close();
	}
}
