package com.run.framework.test.jobs;

import com.run.framework.job.core.Job;
import com.run.framework.job.util.JobUtil;

public class CronJob extends Job {
	private String name;
	
	public CronJob() {
		
	}

	public CronJob(String name) {
		this.name = name;
	}

	@Override
	protected void compute() {

	}

	@Override
	protected void onComplete() {
		System.out.println(name + ": Inside onComplete.");
		System.out.println(JobUtil.nextExecutionDate(this.getCron()));
	}

	@Override
	protected void onFailure() {
		System.out.println(name + ": Inside onFailure");
		System.out.println(JobUtil.nextExecutionDate(this.getCron()));
	}
}
