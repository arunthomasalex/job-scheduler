package com.run.framework.test.jobs;

import java.util.Date;

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
		System.out.println(new Date() + ": Inside compute.");
	}

	@Override
	protected void onComplete() {
		System.out.println(new Date() + ": Inside onComplete.");
		System.out.println(JobUtil.nextExecutionDate(this.getCron()));
	}

	@Override
	protected void onFailure() {
	}
}
