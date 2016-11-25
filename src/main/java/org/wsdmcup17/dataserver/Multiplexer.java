package org.wsdmcup17.dataserver;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wsdmcup17.dataserver.result.Result;
import org.wsdmcup17.dataserver.util.BinaryItem;
import org.wsdmcup17.dataserver.util.SynchronizedBoundedBlockingMapQueue;

public class Multiplexer implements Runnable {
	
	private OutputStream dataStreamPlain;
	
	BlockingQueue<BinaryItem>
		revisionQueue,
		metadataQueue;
	
	private SynchronizedBoundedBlockingMapQueue<Long, Result> mapQueue;
	
	private static final Logger LOG = LoggerFactory.getLogger(Multiplexer.class);
	
	private static final String
		LOG_MSG_END_OF_DOCUMENT = "XML document completely send",
		LOG_MSG_SENDING_REVISION_AT_QUEUE_SIZE =
				"Sending revision %s (queue size %d)";
	
	private long lastMillis = 0;
	
	private static final int
		DELAY = 10000;

	public Multiplexer(OutputStream dataStreamPlain,
			BlockingQueue<BinaryItem> revisionQueue,
			BlockingQueue<BinaryItem> metaDataQueue,
			SynchronizedBoundedBlockingMapQueue<Long, Result> mapQueue) {
		this.dataStreamPlain = dataStreamPlain;
		this.revisionQueue = revisionQueue;
		this.metadataQueue = metaDataQueue;
		this.mapQueue = mapQueue;
	}

	@Override
	public void run() {
		try {
			sendData(dataStreamPlain);
		} catch (InterruptedException | IOException e) {
			Thread.currentThread().interrupt();
			LOG.error("", e);
			throw new RuntimeException(e);
		}
	}
	
	private void sendData(OutputStream dataStreamPlain)
	throws InterruptedException, IOException {
		try(
			// Closing the output stream would result in closing the socket. We
			// prevent this because we may still receive data from the client.
			OutputStream dataStreamShielded =
					new CloseShieldOutputStream(dataStreamPlain);
			DataOutputStream dataStream =
					new DataOutputStream(dataStreamShielded);
		){
			sendData(dataStream);
		}
	}
	
	private void sendData(DataOutputStream dataStream)
	throws InterruptedException, IOException {
		while (!Thread.currentThread().isInterrupted()) {
			BinaryItem revision = revisionQueue.take();
			BinaryItem metadata = metadataQueue.take();
			long revisionId = revision.getRevisionId();
			if (revisionId == Long.MAX_VALUE) {
				LOG.debug(LOG_MSG_END_OF_DOCUMENT);
				break;
			}
			else {
				if (System.currentTimeMillis() - lastMillis > DELAY) {
					LOG.debug(String.format(
						LOG_MSG_SENDING_REVISION_AT_QUEUE_SIZE, revisionId,
						mapQueue.size()));
					lastMillis = System.currentTimeMillis();
				}
				Result result = new Result(revisionId, null);
				mapQueue.put(revision.getRevisionId(), result);
				sendItem(metadata, dataStream);
				sendItem(revision, dataStream);
			}
		}
	}
	
	private void sendItem(BinaryItem item, DataOutputStream dataStream)
	throws IOException {
		dataStream.writeInt(item.getBytes().length);
		dataStream.write(item.getBytes(), 0, item.getBytes().length);
	}

}
