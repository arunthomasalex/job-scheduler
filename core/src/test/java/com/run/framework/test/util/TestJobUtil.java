package com.run.framework.test.util;

import com.run.framework.job.core.Job;
import com.run.framework.job.core.JobState;

public final class TestJobUtil {
	public static void waitForJobState(Job job, JobState state) {
		while(job.getState() != state) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
