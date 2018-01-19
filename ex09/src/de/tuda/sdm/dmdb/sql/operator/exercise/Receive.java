package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

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



		try {
			this.receiveServer = new TCPServer(listenerPort, localCache, finishedPeers);
			Thread serveThread = new Thread(receiveServer);
			serveThread.start();
		} catch (IOException e) {
			System.err.println("FATAL: Could not start receive server on node "+this.nodeId);
		}

		// HINT: local cache must be passed to TCPServer
		//       and will be accessed by multiple Handler-Threads - take multi-threading into account where applicable!

		// init local cache
		this.localCache = new LinkedBlockingDeque<>();

		// Attention: call open on child after starting receive server, so that sendOperator can connect
		System.out.println("Starting  server recv for id "+this.nodeId);
		this.child.open();

	}

	@Override
	public AbstractRecord next() {
		// HINT: local cache must be passed to TCPServer
		//       and will be accessed by multiple Handler-Threads - take multi-threading into account where applicable!
		// process local and received records...

		AbstractRecord nextLocalItem = this.child.next();
		if(nextLocalItem != null){
			return nextLocalItem;
		}

		// check if we finished processing of all records - hint: you can use this.finishedPeers
		if(this.finishedPeers.get() < this.numPeers){


			AbstractRecord nextRemoteItem = null;
			synchronized (localCache) { // make this operation atomic
				try {
					System.out.println("wait blocking");
					nextRemoteItem = ((BlockingQueue<AbstractRecord>)this.localCache).take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if(nextRemoteItem != null){
				return nextRemoteItem;
			} else {
				System.err.println("ERROR: Queue should be filled but poll returned null; this shouldn't happen");
			}
		}

		return null; // all peers and local finished
	}

	@Override
	public void close() {
		// reverse what was done in open()
		this.child.close();
		this.receiveServer.stopServer();
	}

}
