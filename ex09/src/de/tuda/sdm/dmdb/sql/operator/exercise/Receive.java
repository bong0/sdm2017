package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.io.IOException;

import de.tuda.sdm.dmdb.net.TCPServer;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.ReceiveBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

/**
 * Implementation of receive operator
 * @author melhindi
 *
 */
public class Receive extends ReceiveBase {

	/**
	 * Constructor of Receive
	 * @param child - Child operator used to process next calls, usually SendOperator
	 * @param numPeers - Number of peer nodes that have to finish processing before operator finishes
	 * @param listenerPort - Port on which to bind receive server
	 * @param nodeId - Own nodeId, used for debugging
	 */
	public Receive(Operator child, int numPeers, int listenerPort, int nodeId) {
		super(child, numPeers, listenerPort, nodeId);
	}

	@Override
	public void open() {
		// TODO: implement this method
		// HINT: local cache must be passed to TCPServer
		//       and will be accessed by multiple Handler-Threads - take multi-threading into account where applicable!
				
		// init local cache

		// Attention: call open on child after starting receive server, so that sendOperator can connect

	}

	@Override
	public AbstractRecord next() {
		// TODO: implement this method
		// HINT: local cache must be passed to TCPServer
		//       and will be accessed by multiple Handler-Threads - take multi-threading into account where applicable!
		// process local and received records...
		
			// check if we finished processing of all records - hint: you can use this.finishedPeers

		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open()
	}

}
