package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.storage.AbstractPage;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.access.RowIdentifier;
import de.tuda.sdm.dmdb.access.HeapTableBase;
import de.tuda.sdm.dmdb.storage.PageManager;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;

import java.lang.reflect.Constructor;
import java.rmi.UnexpectedException;

public class HeapTable extends HeapTableBase {

	/**
	 * 
	 * Constructs table from record prototype
	 * @param prototypeRecord
	 */

	public HeapTable(AbstractRecord prototypeRecord) {
		super(prototypeRecord);
		this.prototype = prototypeRecord.clone(); // not sure if that's good or redundant

		int currentAttr = 0;
		for(AbstractSQLValue attr:prototypeRecord.getValues()) {
			if (attr == null) {
				throw new IllegalArgumentException("Prototype attribute has unititialized member at position " + currentAttr);
			}
		}

			// instantiate each attribute for prototype by reflection
			/*String nameOfChildClass = attr.getClass().getName();
			try {
				Class<?> classobj = Class.forName(nameOfChildClass);
				AbstractSQLValue newAttr;
				if(attr instanceof SQLVarchar){
					newAttr = (AbstractSQLValue) classobj.newInstance(attr.getMaxLength());
				} else {
					newAttr = (AbstractSQLValue) classobj.newInstance();
				}
				this.prototype.setValue(currentAttr,  newAttr);
			} catch (Exception e){

				e.printStackTrace();
				throw new RuntimeException("Unexpected error: Cannot instantiate blank object of type "+nameOfChildClass+"\n"+e.getMessage());
			}

			currentAttr++;
		}*/
	}

	@Override
	public RowIdentifier insert(AbstractRecord record) {

		int slot = -1;
		try {
			slot = lastPage.insert(record);
		} catch (Exception e) {
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

		AbstractRecord rec = this.getPrototype().clone();
		pg.read(slotNumber, rec);

		return rec;
	}
	
}
