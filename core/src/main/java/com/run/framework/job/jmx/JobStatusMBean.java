package com.run.framework.job.jmx;

import java.util.Map;

public interface JobStatusMBean {
	public Map<String, String> getActiveJobs();
	public Map<String, Map<String, String>> getActiveJobTasks();
}
