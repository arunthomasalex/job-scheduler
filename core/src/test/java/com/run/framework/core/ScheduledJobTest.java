package com.run.framework.core;

import org.junit.Ignore;
import org.junit.Test;

import com.run.framework.job.core.Job;
import com.run.framework.job.core.JobScheduler;
import com.run.framework.job.core.Task;
import com.run.framework.job.util.JobUtil;
import com.run.framework.test.jobs.CronJob;
import com.run.framework.test.jobs.SuccessTask;

public class ScheduledJobTest {
//	@Test
	@Ignore
	public void testCronJob() {
		JobScheduler scheduler = JobScheduler.getDefaultScheduler();
		Job job = new CronJob("cronJob");
		job.setCron("0 0/1 0/1 ? * * *");
		Task task = new SuccessTask("taskLevel-0");
		job.addTask(task);
		task.addDependentTask(new SuccessTask("taskLevel-0-0"));
		scheduler.submit(job);
		System.out.println(JobUtil.nextExecutionDate(job.getCron()));
		try {
			Thread.sleep(60000 * 5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
