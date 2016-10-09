package org.wsdmcup17.dataserver;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {

	private static final Logger
		LOG = LoggerFactory.getLogger(Server.class);
	
	private static final String
		LOG_MSG_LISTENING_ON = "Listening on port %s.";
	
	private static final int
		PARALLELISM = 100;

	private Configuration config;
	
	public Server(Configuration config) {
		this.config = config;
	}

	void start() throws UnknownHostException {
		int port = config.getPort();
		LOG.info(String.format(LOG_MSG_LISTENING_ON, port));
		ExecutorService es = Executors.newWorkStealingPool(PARALLELISM);
		try (ServerSocket serverSocket = new ServerSocket(port)) {
			while (true) {
				Socket clientSocket = serverSocket.accept();
				es.execute(new RequestHandler(config, clientSocket));
			}
		}
		catch (Throwable e) {
			LOG.error("", e);
		}
		finally {
			es.shutdown();
		}
	}
}
