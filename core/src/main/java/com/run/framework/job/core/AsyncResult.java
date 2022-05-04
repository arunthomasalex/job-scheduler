package com.run.framework.job.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.json.JSONObject;

import com.run.framework.job.async.AsyncConstant;
import com.run.framework.job.constant.JobConstant;

/**
 * @author arunalex
 *
 */
public class AsyncResult {
	private static Queue<String> oldTasks = new LinkedList<String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public boolean add(String e) {
			super.add(e);
			if (size() > 1000) {
				this.removeRange(0, size() - 1000);
			}
			return true;
		};
	};
	private static Map<String, Task> tasksMap = new ConcurrentHashMap<String, Task>();
	private static ExecutorService executorService = Executors.newWorkStealingPool(1);

	/**
	 * This is the main static method which is used to register asynchronous task to
	 * wait for response from the task.
	 * 
	 * @param task is the Task object.
	 * @return Nothing.
	 */
	public static void awaitResult(Task task) {
		boolean start = false;
		if (tasksMap.size() <= 0) {
			start = true;
		}
		tasksMap.put(task.getTaskId(), task);
		if (start) {
			try (ServerSocket serverSocket = new ServerSocket(AsyncConstant.SERVER_PORT)) {
				while (tasksMap.size() > 0) {
					Socket socket = serverSocket.accept();
					Consumer<Socket> processResponse = generateConsumerTask();
					executorService.submit(() -> {
						processResponse.accept(socket);
					});
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This is a private method that handles the response from the asynchronous
	 * tasks. This method receives response from the task and send back
	 * acknowledgement as 200 to the task .
	 * 
	 * @param Nothing.
	 * @return Consumer<Socket> lambda object.
	 */
	private static Consumer<Socket> generateConsumerTask() {
		return (socket) -> {
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
				String response = reader.lines().collect(Collectors.joining());
				JSONObject jsonResponse = new JSONObject(response);
				int responseStatus = AsyncConstant.FAILURE_RESPONSE;
				/*
				 * Check if there is response status and the task id is there in the taskMap
				 * then process the response.
				 */
				if (oldTasks.contains(jsonResponse.getString(JobConstant.RESPONSE_TASK))) {
					responseStatus = AsyncConstant.DUPLICATE_REQUEST;
				} else if (jsonResponse.has(JobConstant.RESPONSE_STATUS)) {
					Task task = tasksMap.get(jsonResponse.getString(JobConstant.RESPONSE_TASK));
					if (task != null) {
						if (jsonResponse.getString(JobConstant.RESPONSE_STATUS)
								.equals(JobConstant.RESPONSE_SUCCESS_STATUS)) {
							task.success(jsonResponse.getString(JobConstant.RESPONSE_DATA));
						} else {
							task.failed(jsonResponse.getString(JobConstant.RESPONSE_DATA));
						}
						oldTasks.add(task.getTaskId());
						tasksMap.remove(task.getTaskId());
						responseStatus = AsyncConstant.SUCCESS_RESPONSE;
					}
				}
				// Send the acknowledgement back to task.
				try (Socket clientSocket = new Socket(socket.getInetAddress(), jsonResponse.getInt(AsyncConstant.CLIENT_PORT_RESPONSE))) {
					clientSocket.setOOBInline(true);
					clientSocket.sendUrgentData(responseStatus);
				}
				AsyncResult.checkIfTasksCompleted();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	/**
	 * This is a private method that checks is there is any more task pending to
	 * send the response back, if there is none left then it will send an
	 * acknowledgement back to itself to stop waiting for anymore requests.
	 * 
	 * @param Nothing.
	 * @return Nothing.
	 */
	private static synchronized void checkIfTasksCompleted() {
		if (tasksMap.size() == 0) {
			try (Socket socket = new Socket(InetAddress.getLocalHost(), AsyncConstant.SERVER_PORT)) {
				socket.sendUrgentData(0);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
