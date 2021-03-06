package com.xxl.job.core.rpc.netcom.jetty.server;

import com.xxl.job.core.thread.ExecutorRegistryThread;
import com.xxl.job.core.thread.TriggerCallbackThread;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * rpc jetty server
 * @author xuxueli 2015-11-19 22:29:03
 */
public class JettyServer {
	private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);

	private Server server;
	private Thread thread;

	public void start(final int port, final String ip, final String appName) throws Exception {
		thread = new Thread(new Runnable() {
			@Override
			public void run() {

				// The Server
				// jetty server
				server = new Server(new ExecutorThreadPool());  // 非阻塞

				// HTTP connector
				ServerConnector connector = new ServerConnector(server);
				connector.setPort(port);
				server.setConnectors(new Connector[]{connector});

				// Set a handler
				HandlerCollection handlerc =new HandlerCollection();
				//handler 是接受客户端 http请求  ，处理完成后并相应数据
				handlerc.setHandlers(new Handler[]{new JettyServerHandler()});
				server.setHandler(handlerc);

				try {
					// Start server
					server.start();
					logger.info(">>>>>>>>>>>> xxl-job jetty server start success at port:{}.", port);

					// Start Registry-Server
					//这里面是把 执行器注册到admin 上面
					ExecutorRegistryThread.getInstance().start(port, ip, appName);

					// Start Callback-Server
					TriggerCallbackThread.getInstance().start();

					server.join();	// block until thread stopped
					logger.info(">>>>>>>>>>> xxl-rpc server join success, netcon={}, port={}", JettyServer.class.getName(), port);
				} catch (Exception e) {
					logger.error("", e);
				} finally {
					destroy();
				}
			}
		});
		thread.setDaemon(true);	// daemon, service jvm, user thread leave >>> daemon leave >>> jvm leave
		thread.start();
	}

	public void destroy() {

		// destroy server
		if (server != null) {
			try {
				server.stop();
				server.destroy();
			} catch (Exception e) {
				logger.error("", e);
			}
		}
		if (thread.isAlive()) {
			thread.interrupt();
		}

		// destroy Registry-Server
		ExecutorRegistryThread.getInstance().toStop();

		// destroy Callback-Server
		TriggerCallbackThread.getInstance().toStop();

		logger.info(">>>>>>>>>>> xxl-rpc server destroy success, netcon={}", JettyServer.class.getName());
	}

}
