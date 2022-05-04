package com.run.framework.test.jobs;

import com.run.framework.job.core.Task;

public class AsyncTask  extends Task {
	private String name;

	public AsyncTask(String name) {
		this.name = name;
	}

	@Override
	protected void compute() {
		asyncAwait();
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
