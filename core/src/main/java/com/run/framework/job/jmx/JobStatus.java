package com.run.framework.job.jmx;

import java.util.List;
import java.util.Map;

import com.run.framework.job.core.JobScheduler;

public class JobStatus implements JobStatusMBean {
	private JobScheduler scheduler;

	public JobStatus() {
		this.scheduler = JobScheduler.getDefaultScheduler();
	}

	@Override
	public Map<String, String> getActiveJobs() {
		return scheduler.getActiveJobStatus();
	}

	@Override
	public Map<String, String> getJobschedulerStatus() {
		return scheduler.getJobschedulerStatus();
	}

	@Override
	public List<String> getScheduledJobs() {
		return scheduler.getScheduledJobs();
	}
}
