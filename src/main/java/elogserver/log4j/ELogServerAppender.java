package elogserver.log4j;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 * the elogserver log4j appender
 * 
 * @author haitao
 * 
 */
public class ELogServerAppender extends AppenderSkeleton {

	private String category;

	private String logFileName;

	private String logServerAddress;

	private ServerConnection serverConnection;

	public ELogServerAppender() {
		super();
	}

	@Override
	public void activateOptions() {
		super.activateOptions();
		if (this.category == null || this.category.isEmpty()) {
			throw new RuntimeException("category should not be null");
		}
		if (this.logFileName == null || this.logFileName.isEmpty()) {
			throw new RuntimeException("logFileName should not be null");
		}
		this.serverConnection = new ServerConnection();
	}

	@Override
	protected void append(LoggingEvent event) {
		StringBuilder logBuffer = new StringBuilder(this.layout.format(event));
		if (this.layout.ignoresThrowable()) {
			String[] s = event.getThrowableStrRep();
			if (s != null) {
				for (int i = 0; i < s.length; i++) {
					logBuffer.append(s[i]).append(Layout.LINE_SEP);
				}
			}
		}
		this.serverConnection.writeLog(logBuffer.toString());
		this.serverConnection.flush();
	}

	@Override
	public void close() {
		this.serverConnection.close();
	}

	@Override
	public boolean requiresLayout() {
		return true;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public void setLogFileName(String logFileName) {
		this.logFileName = logFileName;
	}

	public void setLogServerAddress(String logServerAddress) {
		this.logServerAddress = logServerAddress;
	}

	private class ServerConnection {
		private static final String HEADER_DELIMETER = "##";
		private String host;
		private int port;
		private Socket socket = null;
		private DataOutputStream dos = null;
		private volatile boolean alive = false;

		private final ReadWriteLock connectionLock = new ReentrantReadWriteLock();

		private ScheduledExecutorService schedule = Executors
				.newScheduledThreadPool(1);

		ServerConnection() {
			if (logServerAddress == null || logServerAddress.isEmpty()) {
				throw new RuntimeException(
						"logServerAddress should not be null");
			}
			String[] splits = logServerAddress.split(":");
			if (splits.length != 2) {
				throw new RuntimeException(
						"logServerAddress should host:port, value: "
								+ logServerAddress);
			}
			this.host = splits[0];
			this.port = -1;
			try {
				port = Integer.parseInt(splits[1]);
			} catch (NumberFormatException e1) {
				throw new RuntimeException(
						"logServerAddress should host:port, port is number, value : "
								+ logServerAddress);
			}
			connect();
			this.schedule.scheduleWithFixedDelay(new Runnable() {

				@Override
				public void run() {
					connect();
				}

			}, 10, 10, TimeUnit.SECONDS);
		}

		public void flush() {
			try {
				this.dos.flush();
			} catch (IOException e) {
			}
		}

		public void writeLog(String data) {
			if (data == null || data.isEmpty()) {
				return;
			}
			Lock readLock = this.connectionLock.readLock();
			readLock.lock();
			try {
				if (!this.isAlive()) {
					LogLog.debug("elogserver is not connected");
					return;
				}
				byte[] dataBytes = data.getBytes();
				try {
					this.dos.writeInt(RequestHeader.DATA.headerValue);
					this.dos.writeInt(dataBytes.length);
					this.dos.write(dataBytes);
				} catch (IOException e) {
					this.alive = false;
				}
			} finally {
				readLock.unlock();
			}
		}

		private void connect() {
			if (this.isAlive()) {
				return;
			}
			Lock writeLock = this.connectionLock.writeLock();
			writeLock.lock();
			try {
				try {
					if (this.dos != null) {
						this.dos.close();
					}
					if (this.socket != null) {
						this.socket.close();
					}
				} catch (IOException e) {
				}
				this.socket = new Socket();
				try {
					socket.connect(new InetSocketAddress(host, port));
					OutputStream outputStream = socket.getOutputStream();
					this.dos = new DataOutputStream(outputStream);
					this.dos.writeInt(RequestHeader.HANDSHAKE.headerValue);
					byte[] categoryBytes = (ELogServerAppender.this.category
							+ HEADER_DELIMETER + ELogServerAppender.this.logFileName)
							.getBytes();
					this.dos.writeInt(categoryBytes.length);
					this.dos.write(categoryBytes);
					this.dos.flush();
					this.alive = true;
				} catch (IOException e) {
					LogLog.error("failed to connect to elogserver: "
							+ logServerAddress, e);
					this.alive = false;
				}
			} finally {
				writeLock.unlock();
			}

		}
		
		private boolean isAlive(){
			return this.socket != null && this.socket.isConnected() && this.alive;
		}

		public synchronized void close() {
			try {
				if (this.dos != null) {
					this.dos.close();
				}
				if (this.socket != null) {
					this.socket.close();
				}
			} catch (IOException e) {
			}
		}
	}

	enum RequestHeader {

		HANDSHAKE(1), DATA(2);

		private final int headerValue;

		RequestHeader(int value) {
			this.headerValue = value;
		}

		public int getHeaderValue() {
			return headerValue;
		}
	}

}
