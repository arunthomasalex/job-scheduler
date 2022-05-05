package com.run.framework.job.async;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

public class AsyncHandler {
	private ServerSocket clientSocket;
	private InetAddress serverUrl;

	public AsyncHandler() throws IOException {
		serverUrl = InetAddress.getLocalHost();
		clientSocket = new ServerSocket(0);
		clientSocket.setSoTimeout(AsyncConstant.ASYNC_TIMEOUT);
	}
	
	public AsyncHandler(String serverAddress) throws IOException {
		this();
		this.serverUrl = InetAddress.getByName(serverAddress);
	}

	/**
	 * This method is the only public method that receives object that is of type
	 * AsyncResponse to send request to the job scheduler.
	 * 
	 * @param response
	 * @return status
	 */
	public int send(AsyncResponse response) {
		response.setPort(clientSocket.getLocalPort());
		return sendData(response, 0);
	}

	/**
	 * This method is recursive method and is called from
	 * {@link #send(AsyncResponse)} to send the request and then waits for the
	 * response from the server. The request will be retried
	 * <ul>
	 * <li>if there is a ConnectException and trial count is less than
	 * {@link AsyncConstant#ASYNC_RETRY}.</li>
	 * <li>if there is a SocketTimeoutException and trial count is less than
	 * {@link AsyncConstant#ASYNC_RETRY}.</li>
	 * </ul>
	 * 
	 * @param response   AsyncResponse object
	 * @param trialCount int type to keep track of the no of times the response was
	 *                   send.
	 * @return status
	 */
	private int sendData(AsyncResponse response, int trialCount) {
		try {
			try (Socket socket = new Socket(serverUrl, AsyncConstant.SERVER_PORT)) {
				trialCount++;
				try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
					writer.write(response.toString());
					writer.flush();
				}
			}
			// wait for response from the server and throws exception on timeout.
			return waitResponse();
		} catch (UnknownHostException e) {
			return AsyncConstant.FAILURE_RESPONSE;
		} catch (ConnectException e) {
			if (trialCount > AsyncConstant.ASYNC_RETRY) {
				return AsyncConstant.DUPLICATE_REQUEST;
			}
		} catch (SocketTimeoutException e) {
			if (trialCount > AsyncConstant.ASYNC_RETRY) {
				return AsyncConstant.FAILURE_RESPONSE;
			}
		} catch (IOException e) {
			return AsyncConstant.FAILURE_RESPONSE;
		}
		return sendData(response, trialCount);
	}

	/**
	 * This method does the actual wait and throws an exception if an error occur.
	 * 
	 * @return status
	 * @throws IOException
	 */
	private int waitResponse() throws IOException {
		try (Socket socket = clientSocket.accept()) {
			socket.setOOBInline(true);
			return socket.getInputStream().read();
		}
	}
}
