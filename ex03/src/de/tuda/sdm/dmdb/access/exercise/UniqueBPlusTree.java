package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.access.RowIdentifier;
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
		// traverse tree to find index position or that it's missing
		RowIdentifier trvRid = getRidByKeyThroughTraversal(key, root);
		if(trvRid.getSlotNumber() != -1){
			return false; // record key already exists
		}

		// we need to insert into the page returned!
		int destPageId = trvRid.getPageNumber();
		AbstractIndexElement destPage = this.getIndexElement(destPageId);

		if(!destPage.isFull()){
			// insert new leaf into designated leaf page
			destPage.insert(key, record);
		} else {
			AbstractIndexElement newTop = new Node(this);
			addIndexElement

		}
		System.out.println("after: "+this.table.getRecordCount());
		return true;
	}

	/* Returns a row id of index slot */
	private RowIdentifier getRidByKeyThroughTraversal(T key, AbstractIndexElement element){
		int retpos = element.binarySearch(key);
		// if this is a node we're in => fetch child page no pointed to by the record at retpos and recurse
		if(element instanceof Node){
			AbstractRecord tmpRec = this.nodeRecPrototype.clone();
			element.getIndexPage().read(retpos, tmpRec);
			int childPageNo = ((SQLInteger)tmpRec.getValue(PAGE_POS)).getValue();
			getRidByKeyThroughTraversal(key, this.getIndexElement(childPageNo));
		} else {
			if(element.lookup(key) == null){
				// exact key was not found
				return new RowIdentifier(element.getPageNumber(), -1);
			} else {
				return new RowIdentifier(element.getPageNumber(), retpos);
			}
		}
	}

	@Override
	public AbstractRecord lookup(T key) {
		AbstractRecord leafRecFound = root.lookup(key); // does internal recursive binsearch

		// check also if record found matches length of a leaf record
		if(leafRecFound == null || leafRecFound.getValues().length != 3){
			return null;
		}

		SQLInteger pageno = (SQLInteger) leafRecFound.getValue(PAGE_POS);
		SQLInteger slotno = (SQLInteger) leafRecFound.getValue(SLOT_POS);


		// now that we have the rowid => look up actual data
		return this.table.lookup(pageno.getValue(), slotno.getValue());
	}

}
