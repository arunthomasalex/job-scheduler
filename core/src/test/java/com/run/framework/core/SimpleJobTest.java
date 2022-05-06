package com.run.framework.core;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
//import java.util.List;
import java.util.concurrent.CompletableFuture;

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

public class SimpleJobTest {

	@Test
	public void testSuccessJobCreation() throws InterruptedException {
		JobScheduler scheduler = JobScheduler.getDefaultScheduler();
		List<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
		for (int i = 0; i < 10; i++) {
			futures.add(CompletableFuture.supplyAsync(() -> {
				Job job = new SampleJob("successJob");
				Task task = new SuccessTask("sub-0"), sub;
				job.addTask(task);
				sub = new SuccessTask("sub-0-0");
				task.addDependentTasks(Arrays.asList(sub, new SuccessTask("sub-0-1")));
				task = new SuccessTask("sub-0-0-0");
				sub.addDependentTasks(Arrays.asList(task, new SuccessTask("sub-0-0-1"), new SuccessTask("sub-0-0-2")));
				scheduler.submit(job);
				TestJobUtil.waitForJobState(job, JobState.COMPLETED);
				return job;
			}).thenAccept(job -> {
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
			}));
		}
		for(CompletableFuture<Void> future : futures) {
			while(!future.isDone());
		}
	}

	@Test
	public void testFailedJobCreation() throws InterruptedException {
		JobScheduler scheduler = JobScheduler.getDefaultScheduler();
		List<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
		for (int i = 0; i < 10; i++) {
			futures.add(CompletableFuture.supplyAsync(() -> {
				Job job = new SampleJob("failedJob");
				Task task = new SuccessTask("sub-0"), successTask = new SuccessTask("sub-0-0"),
						failedTask = new FailTask("sub-0-0-0");
				task.addDependentTask(successTask);
				successTask.addDependentTask(failedTask);
				failedTask.addDependentTask(new SuccessTask("sub-0-0-0-0"));
				successTask.addDependentTask(new SuccessTask("sub-0-0-1"));
				job.addTask(task);
				scheduler.submit(job);
				TestJobUtil.waitForJobState(job, JobState.FAILED);
				return job;
			}).thenAccept(job -> {
				String data0 = job.getTasks().get(0).getOutput().getData().toString();
				String data00 = job.getTasks().get(0).getTasks().get(0).getOutput().getData().toString();
				String data000 = job.getTasks().get(0).getTasks().get(0).getTasks().get(0).getOutput().getData().toString();
				Object data0000 = job.getTasks().get(0).getTasks().get(0).getTasks().get(0).getTasks().get(0).getOutput();
				String data001 = job.getTasks().get(0).getTasks().get(0).getTasks().get(1).getOutput().getData().toString();
				assertTrue(data0.startsWith("failedJob-failed-sub-0"));
				assertTrue(data0.endsWith("Success"));
				assertTrue(data00.startsWith("failedJob-failed-sub-0-0"));
				assertTrue(data00.endsWith("Success"));
				assertTrue(data000.startsWith("failedJob-failed-sub-0-0-0"));
				assertTrue(data000.endsWith("Failed"));
				assertTrue(data001.startsWith("failedJob-failed-sub-0-0-1"));
				assertTrue(data001.endsWith("Success"));
				assertNull(data0000);
			}));
		}
		for(CompletableFuture<Void> future : futures) {
			while(!future.isDone());
		}
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
