package com.run.framework.job.core;

/**
 * @author arunalex
 *
 */
public class TaskStatus {
	private String jobId;
	private String taskId;
	private TaskState state;
	
	
	public TaskStatus(String jobId, String taskId, TaskState state) {
		super();
		this.jobId = jobId;
		this.taskId = taskId;
		this.state = state;
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

	public TaskState getState() {
		return state;
	}

	public void setState(TaskState state) {
		this.state = state;
	}
}
