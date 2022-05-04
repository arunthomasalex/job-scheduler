package com.run.framework.job.core;

/**
 * @author arunalex
 *
 */
public class TaskOutput {
	private String jobId;
	private String taskId;
	private Object data;

	public TaskOutput(String jobId, String taskId, Object data) {
		this.jobId = jobId;
		this.taskId = taskId;
		this.data = data;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}
}
