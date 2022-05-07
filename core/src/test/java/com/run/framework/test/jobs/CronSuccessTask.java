package com.run.framework.test.jobs;

import com.run.framework.job.core.Task;

public class CronSuccessTask extends Task {

	@Override
	protected void compute() {
		System.out.println(this);
		success(null);
	}

	@Override
	protected void onComplete() {
	}

	@Override
	protected void onFailure() {
	}

}
