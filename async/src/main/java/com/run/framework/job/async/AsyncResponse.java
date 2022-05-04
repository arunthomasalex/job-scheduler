package com.run.framework.job.async;

public class AsyncResponse {
	private String status;
	private String task;
	private String data;
	private int port;
	
	public AsyncResponse() {
		super();
	}
	
	public AsyncResponse(String status, String taskId, String data) {
		this.status = status;
		this.task = taskId;
		this.data = data;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getTask() {
		return task;
	}

	public void setTask(String task) {
		this.task = task;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return String.format("{\"status\":\"%s\",\"task\":\"%s\",\"data\":\"%s\",\"port\":\"%d\"}", status, task, data, port);
	}
}
