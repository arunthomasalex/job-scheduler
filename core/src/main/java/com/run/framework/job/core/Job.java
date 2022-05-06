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
public abstract class Job {
	private String jobId;
	private JobState state;
	private String cron;
	private String prevExec;
	private String nextExec;
	private List<Task> tasks = new ArrayList<Task>();

	public JobState getState() {
		return state;
	}

	public void setState(JobState state) {
		this.state = state;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	
	public void setCron(String cron) {
		this.cron = cron;
	}
	
	public String getCron() {
		return this.cron;
	}
	
	public List<Task> getTasks() {
		return tasks;
	}

	public void addTask(Task task) {
		tasks.add(task);
	}
	
	public void addTasks(Collection<Task> tasks) {
		tasks.addAll(tasks);
	}
	
	public String getPrevExec() {
		return prevExec;
	}

	public void setPrevExec(String prevExec) {
		this.prevExec = prevExec;
	}

	public String getNextExec() {
		return nextExec;
	}

	public void setNextExec(String nextExec) {
		this.nextExec = nextExec;
	}

	public boolean isScheduled() {
		return cron != null && !cron.trim().isEmpty();
	}
	
	protected boolean isSuccessful() {
		return checkAllTaskSuccessful(tasks);
	}

	private boolean checkAllTaskSuccessful(List<Task> jobTasks) {
		for(Task task : jobTasks) {
			if(task.getState() == TaskState.FINISHED) {
				return checkAllTaskSuccessful(task.getTasks());
			} else {
				return false;
			}
		}
		return true;
	}

	protected abstract void compute();

	protected abstract void onComplete();
	
	protected abstract void onFailure();
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "{ jobId:" + jobId + " }";
	}

}
