package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Map;

import de.tuda.sdm.dmdb.net.TCPClient;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.SendBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

/**
 * Implementation of send operator
 * @author melhindi
 *
 */
public class Send extends SendBase {

	/**
	 * Constructor of Send
	 * @param child - Child operator used to process next calls, e.g., TableScan or Selection
	 * @param nodeId - Own nodeId to identify which records to keep locally
	 * @param nodeMap - Map containing connection information (as "IP:port" or "domain-name:port") to establish connection to other peers
	 * @param partitionColumn - Number of column that should be used to repartition the data
	 */

	public Send(Operator child, int nodeId, Map<Integer,String> nodeMap, int partitionColumn) {
		super(child, nodeId, nodeMap, partitionColumn);
	}

	@Override
	public void open() {
		// init child
		child.open(); // init underlying OP (tablescan/selection...)

		// create a client socket for all peer nodes using information in nodeMap
		for(Integer nodeId : nodeMap.keySet()){
			String[] connInfoSplit = nodeMap.get(nodeId).split(":");
			String host = connInfoSplit[0];
			Integer port = Integer.parseInt(connInfoSplit[1]);
			try {
				// store client socket in map for later use
				this.connectionMap.put(nodeId, new TCPClient(host,port));
			} catch (IOException e) {
				System.err.println("FATAL: Could not open connection from peer "+this.nodeId+" to peer "+nodeId);
			}
		}

	}

	@Override
	public AbstractRecord next() {
		// retrieve next from child and determine whether to keep record local or send to peer

		AbstractRecord nextRec = this.child.next();
		if(nextRec == null) {
			// reached end, close connections to peers
			closeConnectionsToPeers();
			return null;
		} // serving records finished

		Integer destinedNodeId = this.getNodeIdForRecord(nextRec, this.partitionColumn);

		if(destinedNodeId != this.nodeId){
			// send to a peer
			this.connectionMap.get(destinedNodeId).sendRecord(nextRec);
		} else {
			return nextRec;
		}


		return null; // record was sent to remote, semantic is changed here; it doesn't indicate that send finished
	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open() - hint there is a helper method that you can use
		System.out.println("Send is closing connections to peers");
		closeConnectionsToPeers();
		System.out.println("Send is closing child");
		this.child.close();
	}

}
