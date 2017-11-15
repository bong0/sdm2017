package de.tuda.sdm.dmdb.sql.statement;

import de.tuda.sdm.dmdb.sql.error.Error;

public class CommitStatement extends Statement {

	@Override
	public Error compile() {
		return Error.NO_ERROR;
	}

	@Override
	public Error execute() {
		return Error.NO_ERROR;
	}

}
