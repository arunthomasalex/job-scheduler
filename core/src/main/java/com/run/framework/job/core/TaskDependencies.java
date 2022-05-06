package com.run.framework.job.core;

public class TaskDependencies {
	private String taskId;
	private String dependentId;
	private String jobId;
	
	public TaskDependencies() {
	}
	
	public TaskDependencies(String taskId, String dependentId, String jobId) {
		this.taskId = taskId;
		this.dependentId = dependentId;
		this.jobId = jobId;
	}
	
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	public String getDependentId() {
		return dependentId;
	}
	public void setDependentId(String dependentId) {
		this.dependentId = dependentId;
	}
	public String getJobId() {
		return jobId;
	}
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	@Override
	public String toString() {
		return "TaskDependencies [taskId=" + taskId + ", dependentId=" + dependentId + ", jobId=" + jobId + "]";
	}
}
