package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

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
		// HINT: local cache must be passed to TCPServer
		//       and will be accessed by multiple Handler-Threads - take multi-threading into account where applicable!
		// init local cache
		this.localCache = new ConcurrentLinkedQueue<AbstractRecord>();

		try {
			this.receiveServer = new TCPServer(listenerPort, localCache, finishedPeers);
			Thread serveThread = new Thread(receiveServer);
			serveThread.start();
		} catch (IOException e) {
			System.err.println("FATAL: Could not start receive server on node "+this.nodeId);
		}

		// Attention: call open on child after starting receive server, so that sendOperator can connect
		this.child.open();

	}

	@Override
	public AbstractRecord next() {
		// HINT: local cache must be passed to TCPServer
		//       and will be accessed by multiple Handler-Threads - take multi-threading into account where applicable!
		// process local and received records...

		// local first!
		AbstractRecord nextLocalItem= this.child.next();
		if(nextLocalItem != null){
			return nextLocalItem;
		}
		//record was sent to remote or local finished

		AbstractRecord nextRemoteItem = null;
		while(true) {
			synchronized(this.localCache) {
				if(!this.localCache.isEmpty()) break;
			}
			// check if we finished processing of all records - hint: you can use this.finishedPeers
			if(this.finishedPeers.get() >= this.numPeers && ((nextLocalItem = this.child.next()) == null)) {
				return null; // all peers and local finished
			}
			if(nextLocalItem != null){
				return nextLocalItem;
			}

			// spin around
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// peeking returned non null, so there is something in our queue!
		synchronized (this.localCache) { // make this operation atomic; Take record out of queue
			nextRemoteItem = (this.localCache).poll();
		}

		if(nextRemoteItem != null){
			return nextRemoteItem; // return item if it's valid
		} else {
			System.err.println("ERROR: Queue should be filled but poll returned null; this shouldn't happen");
		}

		System.out.println("UNEXPECTED BAILOUT");
		return null;
	}

	@Override
	public void close() {
		// reverse what was done in open()
		this.child.close();
		this.receiveServer.stopServer();
	}

}
