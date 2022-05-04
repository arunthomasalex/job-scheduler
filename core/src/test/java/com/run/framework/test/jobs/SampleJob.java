package com.run.framework.test.jobs;

import java.util.List;

import com.run.framework.job.core.Job;
import com.run.framework.job.core.Task;

public class SampleJob extends Job {
	private String name;
	
	public SampleJob() {
		
	}

	public SampleJob(String name) {
		this.name = name;
	}

	@Override
	protected void compute() {

	}

	@Override
	protected void onComplete() {
		appendJobNames(getTasks());
	}

	@Override
	protected void onFailure() {
		appendJobNames(getTasks());
	}

	private void appendJobNames(List<Task> tasks) {
		for (Task task : tasks) {
			if (task.getOutput() != null) {
				task.getOutput().setData(name + "-" + task.getOutput().getData());
				if (task.getTasks().size() > 0) {
					appendJobNames(task.getTasks());
				}
			}
		}
	}
}
