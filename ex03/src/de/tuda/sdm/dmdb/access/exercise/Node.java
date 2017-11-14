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

		int foundPos = this.binarySearch(key);

		try {
			this.indexPage.read(foundPos, nodeRecord);
		} catch (IllegalArgumentException e){
			return null; // page is likely empty
		}

		T keyValue = (T) nodeRecord.getValue(UniqueBPlusTreeBase.KEY_POS);

		if(keyValue.equals(key)){
			return nodeRecord;
		}

		return null;
	}
	
	@Override
	public boolean insert(T key, AbstractRecord record){

		if(lookup(key) != null){
			return false; // record with that key exists
		}
		if(this.isFull()){
			throw new IllegalArgumentException("Node is full, split me");
		}

		this.indexPage.insert(record);
		return true;
	}
	
	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Node<T>(this.uniqueBPlusTree);
	}
	
}