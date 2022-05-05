package com.run.framework.core;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.run.framework.job.async.AsyncHandler;
import com.run.framework.job.async.AsyncResponse;
import com.run.framework.job.constant.JobConstant;
import com.run.framework.job.core.Job;
import com.run.framework.job.core.JobScheduler;
import com.run.framework.job.core.JobState;
import com.run.framework.job.core.Task;
import com.run.framework.test.jobs.AsyncTask;
import com.run.framework.test.jobs.FailTask;
import com.run.framework.test.jobs.SampleJob;
import com.run.framework.test.jobs.SuccessTask;
import com.run.framework.test.util.TestJobUtil;

public class JobSchedulerTest {

	@Test
	public void testSuccessJobCreation() throws InterruptedException {
		JobScheduler scheduler = JobScheduler.getDefaultScheduler();
		Job job = new SampleJob("successJob");
		Task task = new SuccessTask("sub-0"), sub;
		job.addTask(task);
		sub = new SuccessTask("sub-0-0");
		task.addDependentTasks(Arrays.asList(sub, new SuccessTask("sub-0-1")));
		task = new SuccessTask("sub-0-0-0");
		sub.addDependentTasks(Arrays.asList(task, new SuccessTask("sub-0-0-1"), new SuccessTask("sub-0-0-2")));
		scheduler.submit(job);
		TestJobUtil.waitForJobState(job, JobState.COMPLETED);
		String data0 = job.getTasks().get(0).getOutput().getData().toString();
		String data00 = job.getTasks().get(0).getTasks().get(0).getOutput().getData().toString();
		String data01 = job.getTasks().get(0).getTasks().get(1).getOutput().getData().toString();
		String data000 = job.getTasks().get(0).getTasks().get(0).getTasks().get(0).getOutput().getData().toString();
		String data001 = job.getTasks().get(0).getTasks().get(0).getTasks().get(1).getOutput().getData().toString();
		String data002 = job.getTasks().get(0).getTasks().get(0).getTasks().get(2).getOutput().getData().toString();
		assertTrue(data0.contains("successJob-sub-0"));
		assertTrue(data0.contains("Success"));
		assertTrue(data00.contains("successJob-sub-0-0"));
		assertTrue(data00.contains("Success"));
		assertTrue(data01.contains("successJob-sub-0-1"));
		assertTrue(data01.contains("Success"));
		assertTrue(data000.contains("successJob-sub-0-0-0"));
		assertTrue(data000.contains("Success"));
		assertTrue(data001.contains("successJob-sub-0-0-1"));
		assertTrue(data001.contains("Success"));
		assertTrue(data002.contains("successJob-sub-0-0-2"));
		assertTrue(data002.contains("Success"));
	}

	private void printAllOutputs(List<Task> tasks) {
		for (Task task : tasks) {
			if (task.getOutput() != null) {
				System.out.println(task.getOutput().getData());
			}
			printAllOutputs(task.getTasks());
		}
	}
	
	private void printJobTasksOutput(Job job) {
		printAllOutputs(job.getTasks());
	}

	@Test
	public void testFailedJobCreation() throws InterruptedException {
		for (int i = 0; i < 10; i++)
			testFailedJobCreationmultiple(i);
	}

	private void testFailedJobCreationmultiple(int i) {
		JobScheduler scheduler = JobScheduler.getDefaultScheduler();
		Job job = new SampleJob(i + "-failedJob");
		Task task = new SuccessTask("sub-0"), successTask = new SuccessTask("sub-1"),
				failedTask = new FailTask("sub-2-0");
		task.addDependentTask(successTask);
		successTask.addDependentTask(failedTask);
		failedTask.addDependentTask(new SuccessTask("sub-3"));
		successTask.addDependentTask(new SuccessTask("sub-2-1"));
		job.addTask(task);
		scheduler.submit(job);
		TestJobUtil.waitForJobState(job, JobState.FAILED);
		String data0 = job.getTasks().get(0).getOutput().getData().toString();
		String data1 = job.getTasks().get(0).getTasks().get(0).getOutput().getData().toString();
		String data20 = job.getTasks().get(0).getTasks().get(0).getTasks().get(0).getOutput().getData().toString();
		Object outputObj = job.getTasks().get(0).getTasks().get(0).getTasks().get(0).getTasks().get(0).getOutput();
		String data21 = job.getTasks().get(0).getTasks().get(0).getTasks().get(1).getOutput().getData().toString();
		assertTrue(data0.startsWith(i + "-failedJob-sub-0"));
		assertTrue(data0.endsWith("Success"));
		assertTrue(data1.startsWith(i + "-failedJob-sub-1"));
		assertTrue(data1.endsWith("Success"));
		assertTrue(data20.startsWith(i + "-failedJob-sub-2-0"));
		assertTrue(data20.endsWith("Failed"));
		assertTrue(data21.startsWith(i + "-failedJob-sub-2-1"));
		assertTrue(data21.endsWith("Success"));
		assertNull(outputObj);
		printJobTasksOutput(job);
	}

	@Test
	public void testAsyncJobCreation() {
		JobScheduler scheduler = JobScheduler.getDefaultScheduler();
		Job job = new SampleJob("asyncJob");
		Task task = new AsyncTask("sub");
		job.addTask(task);
		scheduler.submit(job);
		handleAsyncResponse(task);
		TestJobUtil.waitForJobState(job, JobState.COMPLETED);
		String data = job.getTasks().get(0).getOutput().getData().toString();
		assertTrue(data.contains("asyncJob-sub"));
		assertTrue(data.contains("Async Success"));
	}

	private void handleAsyncResponse(Task task) {
		AsyncResponse response = new AsyncResponse(JobConstant.RESPONSE_SUCCESS_STATUS, task.getTaskId(),
				"Async Success");
		try {
			AsyncHandler handler = new AsyncHandler();
			handler.send(response);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
