package com.run.framework.test.jobs;

import com.run.framework.job.core.Task;

public class FailTask extends Task {
	private String name;

	public FailTask(String name) {
		this.name = name;
	}

	@Override
	protected void compute() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		failed("Failed");
	}

	@Override
	protected void onComplete() {
		getOutput().setData(name + "=" + getOutput().getData());
	}

	@Override
	protected void onFailure() {
		getOutput().setData(name + "=" + getOutput().getData());
	}

}