package com.run.framework.job.jmx;

import java.util.stream.Collectors;

import com.run.framework.job.core.JobScheduler;

public class JobStatus implements JobStatusMBean {

	@Override
	public String getActiveJobs() {
		JobScheduler scheduler = JobScheduler.getDefaultScheduler();
		return scheduler.getActiveJobs().stream().map(job -> job.getClass().getCanonicalName() + "(" + job.getJobId() + ")").collect(Collectors.joining(","));
	}
}
