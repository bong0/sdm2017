package de.tuda.sdm.dmdb.storage;

import java.io.Serializable;
import java.util.Vector;

import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

/**
 * Defines the interface for a record stored in a page
 * @author cbinnig
 *
 */
public abstract class AbstractRecord implements Cloneable, Serializable{
	
	private static final long serialVersionUID = 1L;
	protected AbstractSQLValue[] values;
	
	/**
	 * Creates a record with a given number of attributes
	 * @param length number of attributes
	 */
	public AbstractRecord(int attNumber){
		this.values = new AbstractSQLValue[attNumber];
	}
	
	/**
	 * Set value i of record
	 * @param i
	 * @param value
	 */
	public void setValue(int i, AbstractSQLValue value){
		this.values[i] = value;
	}
	
	/**
	 * Return value of attribute with a given number
	 * @param i attribute number
	 * @return
	 */
	public AbstractSQLValue getValue(int i){
		return this.values[i];
	}
	
	/**
	 * Returns all values of the recors as array
	 * @return
	 */
	public AbstractSQLValue[] getValues(){
		return this.values;
	}
	
	/**
	 * Keeps only listed column numbers
	 * @param colNumbers
	 */
	public void keepValues(Vector<Integer> colNumbers){
		AbstractSQLValue[] tmpValues = this.values;
		
		this.values = new AbstractSQLValue[colNumbers.size()];
		int i=0;
		for(Integer colNum: colNumbers){
			this.values[i++] = tmpValues[colNum];
		}
	}
	
	/**
	 * Returns the fixed size of a record needed for for storing 
	 * in a slot in a page
	 * @return
	 */
	public abstract int getFixedLength();
	
	/**
	 * Returns the variable size of a record needed for storing 
	 * the record the variable part in a page
	 * @return
	 */
	public abstract int getVariableLength();

	/**
	 * Creates a new record form current record and rec by 
	 * appending rec to the end of the current record
	 * @param rec
	 * @return
	 */
	public abstract AbstractRecord append(AbstractRecord rec);
	
	@Override
	public abstract AbstractRecord clone();
	
	@Override
	public boolean equals(Object o) {
		//check for self-comparison
	    if ( this == o ) return true;

	    //use instanceof instead of getClass here for two reasons
	    //1. if need be, it can match any supertype, and not just one class;
	    //2. it renders an explict check for "that == null" redundant, since
	    //it does the check for null already - "null instanceof [type]" always
	    //returns false. (See Effective Java by Joshua Bloch.)
	    if ( !(o instanceof AbstractRecord) ) return false;
	    
	    AbstractRecord cmp = (AbstractRecord)o;

		if(this.values.length != cmp.getValues().length)
			return false;

		int i=0;
		for(AbstractSQLValue cmpValue: cmp.getValues()){
			//check if types match
			if(cmpValue.getType()!=this.values[i].getType())
				return false;

			//check if values match
			if(!cmpValue.equals(this.values[i]))
				return false;

			i++;
		}
		return true;

	}
}
