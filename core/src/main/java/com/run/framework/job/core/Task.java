/**
 * 
 */
package com.run.framework.job.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author arunalex
 *
 */
public abstract class Task implements Runnable {
	private String taskId;
	private String jobId;
	private List<Task> dependentTasks = new ArrayList<Task>();
	private TaskOutput output;
	private TaskState taskState;

	public TaskState getTaskState() {
		return taskState;
	}
	
	public String getTaskId() {
		return taskId;
	}
	
	public String getJobId() {
		return jobId;
	}

	public void addDependentTask(Task task) {
		this.dependentTasks.add(task);
	}
	
	public void addDependentTasks(Collection<Task> tasks) {
		this.dependentTasks.addAll(tasks);
	}
	
	public void addReverseDependentTasks(Collection<Task> tasks) {
		for(Task task : tasks)
			task.addDependentTask(this);
	}
	
	public TaskOutput getOutput() {
		return output;
	}
	
	public void setTaskState(TaskState state) {
		this.taskState = state;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	protected void setOutput(TaskOutput output) {
		this.output = output;
	}
	
	public List<Task> getTasks() {
		return this.dependentTasks;
	}

	public void success(Object data) {
		output = new TaskOutput(jobId, taskId, data);
		taskState =  TaskState.EXECUTED;
	}
	
	public void failed(Object data) {
		output = new TaskOutput(jobId, taskId, data);
		taskState = TaskState.FAILING;
	}
	
	public void asyncAwait() {
		AsyncResult.awaitResult(this);
	}
	
	protected abstract void compute();

	protected abstract void onComplete();
	
	protected abstract void onFailure();
	
	@Override
	public void run() {
		taskState = TaskState.STARTED;
		this.compute();
	}
}
