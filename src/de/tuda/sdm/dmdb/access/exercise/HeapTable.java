package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.storage.AbstractPage;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.access.RowIdentifier;
import de.tuda.sdm.dmdb.access.HeapTableBase;
import de.tuda.sdm.dmdb.storage.PageManager;
import de.tuda.sdm.dmdb.storage.Record;

import java.rmi.UnexpectedException;

public class HeapTable extends HeapTableBase {

	/**
	 * 
	 * Constructs table from record prototype
	 * @param prototypeRecord
	 */
	public HeapTable(AbstractRecord prototypeRecord) {
		super(prototypeRecord);
	}

	@Override
	public RowIdentifier insert(AbstractRecord record) {

		System.out.println(this.lastPage.getPageNumber());
		int slot = -1;
		try {
			slot = lastPage.insert(record);
		} catch (Exception e) {
			e.printStackTrace();
			// page is FULL, create new one
			this.lastPage = PageManager.createDefaultPage(this.prototype.getFixedLength());
			this.addPage(this.lastPage);
			try {
				slot = this.lastPage.insert(record);
			} catch (Exception e2){
				throw new RuntimeException("Unexpected error: could not write record into newly created, blank, page");
			}
		}
		this.recordCount++;
		return new RowIdentifier(this.lastPage.getPageNumber(), slot);
	}

	@Override
	public AbstractRecord lookup(int pageNumber, int slotNumber) {
		AbstractPage pg;
		try {
			pg = this.pages.get(pageNumber);
		} catch (Exception e){
			throw new IllegalArgumentException("Pagenumber not found/invalid");
		}

		AbstractRecord rec = null;
		try {
			rec = new Record(this.prototype.getValues().length);
			pg.read(slotNumber, rec);
		} catch (Exception e){
			throw new IllegalArgumentException("Slotnumber not found/invalid");
		}

		return rec;
	}
	
}
