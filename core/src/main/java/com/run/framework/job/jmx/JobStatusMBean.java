package com.run.framework.job.jmx;

import java.util.List;
import java.util.Map;

public interface JobStatusMBean {
	public List<String> getScheduledJobs();
	public Map<String, String> getActiveJobs();
	public Map<String, String> getJobschedulerStatus();
}
