package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.access.AbstractIndexElement;
import de.tuda.sdm.dmdb.access.NodeBase;
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
		
		return true;
	}
	
	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Node<T>(this.uniqueBPlusTree);
	}
	
}