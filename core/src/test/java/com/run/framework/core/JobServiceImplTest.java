package com.run.framework.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.run.framework.job.core.Job;
import com.run.framework.job.core.JobService;
import com.run.framework.job.core.JobServiceImpl;
import com.run.framework.job.core.JobState;
import com.run.framework.job.core.Task;
import com.run.framework.job.core.TaskState;
import com.run.framework.test.jobs.SampleJob;
import com.run.framework.test.jobs.SuccessTask;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JobServiceImplTest {
	
	@Test
	public void testcase_1_job_add() {
		Job job = new SampleJob();
		job.setJobId("123");
		job.setState(JobState.STARTED);
		JobService jobService = new JobServiceImpl();
		boolean status = jobService.insert(job);
		assertTrue(status);
	}
	
	@Test
	public void testcase_2_job_fetch() {
		JobService jobService = new JobServiceImpl();
		Job job = jobService.read("123");
		assertEquals(job.getJobId(), "123");
	}
	
	@Test
	public void testcase_3_job_remove() {
		JobService jobService = new JobServiceImpl();
		boolean status = jobService.delete("123");
		assertTrue(status);
	}
	
	@Test
	public void testcase_4_task_add() {
		Task task = new SuccessTask();
		task.setTaskId("12");
		task.setJobId("123");
		task.setState(TaskState.STARTED);
		JobService jobService = new JobServiceImpl();
		boolean status = jobService.insertTask(task);
		assertTrue(status);
	}
	
	@Test
	public void testcase_5_task_fetch() {
		JobService jobService = new JobServiceImpl();
		Task task = jobService.readTask("12");
		assertEquals(task.getTaskId(), "12");
		assertEquals(task.getJobId(), "123");
	}
	
	@Test
	public void testcase_6_task_remove() {
		JobService jobService = new JobServiceImpl();
		boolean status = jobService.deleteTask("123");
		assertTrue(status);
	}
	
	@Test
	public void testcase_7_tasks_add_fetch() {
		JobService jobService = new JobServiceImpl();
		Task task = new SuccessTask();
		task.setTaskId("1");
		task.setJobId("1");
		task.setState(TaskState.STARTED);
		jobService.insertTask(task);
		task = new SuccessTask();
		task.setTaskId("2");
		task.setJobId("1");
		task.setState(TaskState.STARTED);
		jobService.insertTask(task);
		List<Task> tasks = jobService.readTasks("1");
		assertEquals(tasks.size(), 2);
	}
	
	@Test
	public void testcase_8_tasks_remove() {
		JobService jobService = new JobServiceImpl();
		boolean status = jobService.deleteTasks("1");
		assertTrue(status);
		List<Task> tasks = jobService.readTasks("1");
		assertEquals(tasks.size(), 0);
	}
}
