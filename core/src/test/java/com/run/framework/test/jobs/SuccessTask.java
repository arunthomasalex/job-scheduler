package com.run.framework.test.jobs;

import com.run.framework.job.core.Task;

public class SuccessTask extends Task {
	private String name;

	public SuccessTask() {
		// TODO Auto-generated constructor stub
	}
	
	public SuccessTask(String name) {
		this.name = name;
	}

	@Override
	protected void compute() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		success("Success");
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
