package de.tuda.sdm.dmdb.sql.statement;

import de.tuda.sdm.dmdb.sql.error.Error;

public abstract class Statement {
	protected StatementType type;

	public StatementType getType() {
		return type;
	}

	public void setType(StatementType type) {
		this.type = type;
	}
	
	public boolean isDDL(){
		switch(this.type){
		case CREATE_INDEX:
		case CREATE_TABLE:
			return true;
		default:
			break;
		}
		return false;
	}
	
	public boolean isDML(){
		switch(this.type){
		case SELECT:
		case INSERT:
			return true;
		default:
			break;
		}
		return false;
	}
	
	public abstract Error compile();
	
	public abstract Error execute();
}
