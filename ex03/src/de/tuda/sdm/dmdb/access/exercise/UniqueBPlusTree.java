package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.access.AbstractIndexElement;
import de.tuda.sdm.dmdb.access.exercise.Node;
import de.tuda.sdm.dmdb.catalog.objects.Index;
import de.tuda.sdm.dmdb.storage.AbstractPage;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.SQLInteger;

/**
 * Unique B+-Tree implementation 
 * @author cbinnig
 *
 * @param <T>
 */
public class UniqueBPlusTree<T extends AbstractSQLValue> extends UniqueBPlusTreeBase<T> {
	
	/**
	 * Constructor of B+-Tree with user-defined fill-grade
	 * @param table Table to be indexed
	 * @param keyColumnNumber Number of unique column which should be indexed
	 * @param fillGrade fill grade of index
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber, int fillGrade) {
		super(table, keyColumnNumber, fillGrade);
	} 
	
	/**
	 * Constructor for B+-tree with default fill grade
	 * @param table table to be indexed 
	 * @param keyNumber Number of unique column which should be indexed
	 */
	public UniqueBPlusTree(AbstractTable table, int keyColumnNumber) {
		this(table, keyColumnNumber, DEFAULT_FILL_GRADE);
	}	
	
	@SuppressWarnings({ "unchecked" })
	@Override
	public boolean insert(AbstractRecord record) {

		T key = (T) record.getValue(this.keyColumnNumber);
		if(this.lookup(key) != null){
			return false; // record key already exists
		}

		AbstractIndexElement destPage=null;
		// determine page to insert in
		// case: root is not yet an inner node, all keys are on one leaf/level
		if(this.getRoot() instanceof Leaf) {
			destPage = this.getRoot();
			System.out.println("After adding: "+key+"---"+record.toString());
		} else {
			
			// Traverse tree recursively finding the last inner node ...
			// that is responsible for the range the key lies in
			// use helper method?
		}

		try {
			// insert new leaf into designated leaf page
			destPage.insert(key, record);
		} catch (Exception e){
			System.out.println("Uncaught Exception, not implemented... ");
			e.printStackTrace();
			// case: we need to create a new page since it's full
			// incorporate page into indexpage map and setup linking in tree
		}


		return true;
	}
	
	@Override
	public AbstractRecord lookup(T key) {
		AbstractRecord leafRecFound = root.lookup(key); // does internal recursive binsearch

		root.print();
		// check also if record found matches length of a leaf record
		if(leafRecFound == null || leafRecFound.getValues().length != 3){
			System.out.println("index lookup returned: "+leafRecFound);
			return null;
		}

		SQLInteger pageno = (SQLInteger) leafRecFound.getValue(PAGE_POS);
		SQLInteger slotno = (SQLInteger) leafRecFound.getValue(SLOT_POS);


		// now that we have the rowid => look up actual data
		System.out.println("Page No."+pageno.getValue()+"SlotNo. "+slotno.getValue());
		return this.table.lookup(pageno.getValue(), slotno.getValue());
	}

}
