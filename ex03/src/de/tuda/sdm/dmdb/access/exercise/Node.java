package de.tuda.sdm.dmdb.access.exercise;

import java.net.NetworkInterface;

import de.tuda.sdm.dmdb.access.AbstractIndexElement;
import de.tuda.sdm.dmdb.access.NodeBase;
import de.tuda.sdm.dmdb.access.RowIdentifier;
import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.SQLInteger;

/**
 * Index node
 * @author cbinnig
 *
 */
public class Node<T extends AbstractSQLValue> extends NodeBase<T>{

	/**
	 * Node constructor
	 * @param uniqueBPlusTree TODO
	 */
	public Node(UniqueBPlusTreeBase<T> uniqueBPlusTree){
		super(uniqueBPlusTree);
	}
	
	
	@Override
	public AbstractRecord lookup(T key) {

		AbstractRecord nodeRecord = this.uniqueBPlusTree.getNodeRecPrototype().clone();

		// FIXME use this.binarySearch(); instead
		//int nextIndex = this.binarySearch(key);
		

		for(int i=0; i<this.indexPage.getNumRecords();++i){
			this.indexPage.read(i, nodeRecord);

			SQLInteger keyValue = (SQLInteger)nodeRecord.getValue(UniqueBPlusTreeBase.KEY_POS);

			if(keyValue == key){
				return nodeRecord;
			}
		}

		return null;
	}
	
	@Override
	public boolean insert(T key, AbstractRecord record){
		//TODO: implement this method
		if(this.isFull()){
			//split the nodes
		}
		if(lookup(key) != null){
			return false; // record with that key exists
		}
		RowIdentifier newRid = this.uniqueBPlusTree.getTable().insert(record);
		AbstractRecord nodeRec = this.uniqueBPlusTree.getNodeRecPrototype().clone();
		nodeRec.setValue(UniqueBPlusTreeBase.KEY_POS, key);
		nodeRec.setValue(UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(newRid.getPageNumber()));
		
		this.indexPage.insert(nodeRec);
		return true;
	}
	
	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Node<T>(this.uniqueBPlusTree);
	}
	
}