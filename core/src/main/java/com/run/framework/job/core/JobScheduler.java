/**
 * 
 */
package com.run.framework.job.core;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.run.framework.job.jmx.JobConfig;
import com.run.framework.job.jmx.JobStatus;
import com.run.framework.job.util.JobUtil;

/**
 * @author arunalex
 *
 */
public class JobScheduler {

	private static JobScheduler defaultScheduler = new JobScheduler("Default");
	private String schedulerName;
	private List<Job> jobs;
	private Map<String, List<String>> taskDependencies;
	private Lock daemonLock, jobLock;
	private JobService jobService;
	private static MBeanServer mBeanServer;
	private ExecutorService executorService = Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors());

	/**
	 * Handler for the daemon job that can be used to start and stop the daemon job.
	 */
	private ScheduledFuture<?> daemon;

	/**
	 * This scheduler is used handling cron job that are scheduled for execution in
	 * future and also the daemon task that is initialized in
	 * {@link #startScheduler()}.
	 */
	private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

	private JobConfiguration configuration;

	static {
		mBeanServer = ManagementFactory.getPlatformMBeanServer();
		ObjectName jobStatus = null;
		ObjectName jobConfig = null;
		try {
			jobStatus = new ObjectName("com.run.framework.jmx:type=JobDetails");
			jobConfig = new ObjectName("com.run.framework.jmx:type=JobConfig,group=configuration");
			mBeanServer.registerMBean(new JobStatus(), jobStatus);
			mBeanServer.registerMBean(new JobConfig(), jobConfig);
		} catch (MalformedObjectNameException e) {
			e.printStackTrace();
		} catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			e.printStackTrace();
		}
	}

	private JobScheduler(String name) {
		schedulerName = name;
		configuration = JobConfiguration.getConfigurations();
		jobService = new JobServiceImpl();
		daemonLock = new ReentrantLock();
		jobLock = new ReentrantLock();
		jobs = Collections.synchronizedList(new ArrayList<Job>());
		taskDependencies = new ConcurrentHashMap<String, List<String>>();
		startScheduler();
	}

	public void startScheduler() {
		if (daemon == null || daemon.isDone()) {
			loadScheduledJobs();
			changeJobsStatus(JobState.STALLED, JobState.EXECUTING);
			daemon = scheduledExecutorService.scheduleAtFixedRate(() -> {
				boolean isLockAvailable = false;
				try {
					isLockAvailable = daemonLock.tryLock(1, TimeUnit.SECONDS);
					if (isLockAvailable) {
						jobLock.lock();
						ListIterator<Job> jobIter = jobs.listIterator();
						while (jobIter.hasNext()) {
							Job job = jobIter.next();
							boolean jobCompleted = true;
							for (Task task : job.getTasks()) {
								boolean status = createSubTasksIfDone(task, job);
								if (!status) {
									jobCompleted = status;
								}
							}
							if (jobCompleted) {
								if (job.isSuccessful()) {
									job.onComplete();
									changeJobStatus(job, JobState.COMPLETED, job.isScheduled());
								} else {
									job.onFailure();
									changeJobStatus(job, JobState.FAILED, job.isScheduled());
								}
								removeAllTaskDependencies(job.getTasks());
								jobIter.remove();
							}
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					if (isLockAvailable) {
						daemonLock.unlock();
						jobLock.unlock();
					}
				}
			}, 0, Long.valueOf(configuration.getProperty("config.daemon.interval", "1")), TimeUnit.SECONDS);
		}
	}

	private void loadScheduledJobs() {
		Set<String> loadedJobs = jobs.stream().map(Job::getJobId).collect(Collectors.toSet());
		List<Job> persistedJobs = jobService.readAll();
		for (Job job : persistedJobs) {
			if (job.isScheduled() && !loadedJobs.contains(job.getJobId())) {
				Map<String, Task> jobTasks = new HashMap<String, Task>();
				for (Task task : jobService.readTasks(job.getJobId())) {
					jobTasks.put(task.getTaskId(), task);
				}
				Map<String, Set<String>> taskDep = jobService.readJobTaskDependencies(job.getJobId()).stream()
						.collect(Collectors.groupingBy(TaskDependencies::getTaskId,
								Collectors.mapping(TaskDependencies::getDependentId, Collectors.toSet())));
				loadJobTask(job.getTasks(), jobTasks, taskDep, "0");
				submit(job);
			}
		}
	}

	private void loadJobTask(List<Task> tasks, Map<String, Task> jobTasks, Map<String, Set<String>> taskDep,
			String taskId) {
		Set<String> taskIds = taskDep.get(taskId);
		if (taskIds != null)
			for (String id : taskIds) {
				Task task = jobTasks.get(id);
				tasks.add(task);
				loadJobTask(task.getTasks(), jobTasks, taskDep, task.getTaskId());
			}
	}

	private void stopScheduler(boolean flag) {
		daemon.cancel(flag);
		changeJobsStatus(JobState.EXECUTING, JobState.STALLED);
	}

	public void stopScheduler() {
		if (daemon != null && !daemon.isDone()) {
			stopScheduler(false);
		}
	}

	public void forceStopScheduler() {
		if (daemon != null && !daemon.isDone()) {
			stopScheduler(true);
		}
	}

	private void changeJobsStatus(JobState from, JobState to) {
		jobs.stream().filter(job -> job.getState() == from).forEach(job -> {
			job.setState(to);
			changeJobStatus(job, to, job.isScheduled());
		});
	}

	/**
	 * This method will remove any remaining objects from {@link #taskDependencies}
	 * after the complete job execution.
	 * 
	 * @param tasks List<Task> object
	 */
	private void removeAllTaskDependencies(List<Task> tasks) {
		for (Task task : tasks)
			taskDependencies.remove(task.getTaskId());
	}

	/**
	 * This method will be called at a fixed interval to handle the state of the
	 * tasks. Inside this method there is the logic to perform action on each state
	 * change of the task.
	 * <ul>
	 * <li>Some TaskState are handled from inside Task class
	 * <ul>
	 * <li>{@value TaskState#EXECUTED}
	 * <li></li>{@value TaskState#STARTED}
	 * <li></li>{@value TaskState#FAILED}</li>
	 * </ul>
	 * </li>
	 * <li>The rest to TaskState is handled from this method.</li>
	 * <ul>
	 * 
	 * @param task Task object
	 * @param job  Job object
	 * @return boolean "true" if the task is finished and there are no sub tasks
	 *         remaining or if a task failed.
	 */
	private boolean createSubTasksIfDone(Task task, Job job) {
		if (task.getState() == TaskState.STARTED) {
			changeTaskStatus(task, TaskState.EXECUTING, job.isScheduled());
		} else if (task.getState() == TaskState.EXECUTED) {
			updateTaskStatus(task, job.isScheduled());
			task.onComplete();
			changeTaskStatus(task, TaskState.COMPLETED, job.isScheduled());
			removeFromTaskDependencies(task, task.getTasks());
			initiateTasks(task.getTasks());
		} else if (task.getState() == TaskState.COMPLETED) {
			changeTaskStatus(task, TaskState.FINISHED, job.isScheduled());
			return createSubTasksIfDone(task, job);
		} else if (task.getState() == TaskState.FINISHED) {
			boolean status = true;
			for (Task sub : task.getTasks()) {
				status = createSubTasksIfDone(sub, job);
				if (!status)
					return status;
			}
		} else if (task.getState() == TaskState.FAILING) {
			task.onFailure();
			changeTaskStatus(task, TaskState.FAILED, job.isScheduled());
		}
		return task.getState() == TaskState.FINISHED || task.getState() == TaskState.FAILED;
	}

	/**
	 * This method will remove the dependent tasks that successfully finished
	 * execution from {@link #taskDependencies} object.
	 * 
	 * @param task  Task object
	 * @param tasks List<Task> Object
	 */
	private void removeFromTaskDependencies(Task task, List<Task> tasks) {
		for (Task tsk : tasks)
			taskDependencies.get(tsk.getTaskId()).remove(task.getTaskId());
	}

	/**
	 * This method is used to schedule the job passed to this method using the
	 * {@link JobUtil#nextExecutionTime(cronExpression)} to calculate the schedule
	 * time.
	 * 
	 * @param job       Job object
	 * @param scheduled boolean variable
	 */
	private void scheduleJob(Job job, boolean scheduled) {
		if (scheduled) {
			scheduledExecutorService.schedule(() -> initiateJob(job), JobUtil.nextExecutionTime(job.getCron()),
					TimeUnit.MILLISECONDS);
		} else {
			initiateJob(job);
		}
	}

	/**
	 * This method will first call the {@link #addJob()} method and then calls
	 * {@link #processJob(Job)} and then initiate the tasks in the jobs where the
	 * task state will be set to TaskState.STARTED. After assigning all the task to
	 * execute the job will be checked if it is a scheduled job, if so then the job
	 * will be rescheduled to execute in the next scheduled time.
	 * 
	 * @param job Job object
	 */
	private void initiateJob(Job job) {
		addJob(job);
		processJob(job);
		initiateTasks(job.getTasks());
		if (job.isScheduled()) {
			scheduleJob(job, true);
		}
	}

	/**
	 * This method will start the task threads.
	 * 
	 * @param tasks
	 */
	private void initiateTasks(List<Task> tasks) {
		for (Task task : tasks) {
			executorService.submit(task); // Task will be in started state.
		}
	}

	/**
	 * This method will execute the compute method of a job which start the
	 * execution of job and change the state to {@value JobState#EXECUTING}.
	 * 
	 * @param job Job object
	 */
	private void processJob(Job job) {
		job.compute();
		changeJobStatus(job, JobState.EXECUTING, job.isScheduled());
	}

	/**
	 * This method change the state of a new job to {@value JobState#STARTED} and
	 * add the job into "jobs" list for execution.
	 * 
	 * @param job Job object
	 */
	private void addJob(Job job) {
		changeJobStatus(job, JobState.STARTED, job.isScheduled());
		jobs.add(job);
	}

	/**
	 * This method will set the job state and send the job to updateJobStatus to
	 * update the new state in database.
	 * 
	 * @param job   Job object
	 * @param state new job state
	 * @param b
	 */
	private void changeJobStatus(Job job, JobState state, boolean scheduled) {
		job.setState(state);
		updateJobStatus(job, scheduled);
	}

	/**
	 * This method is used to add the job details to the database.
	 * 
	 * @param job
	 * @param scheduled
	 */
	private void updateJobStatus(Job job, boolean scheduled) {
		if (job.getState() == JobState.STARTED) {
			job.setPrevExec(job.getNextExec());
			if (scheduled) {
				job.setNextExec(JobUtil.nextExecutionDate(job.getCron()));
				Job tmp = jobService.read(job.getJobId());
				if (tmp != null) {
					jobService.update(job);
					return;
				}
			}
			jobService.insert(job);
			addTaskDependencies(job.getTasks(), "0", job.getJobId());
		} else if (job.getState() == JobState.COMPLETED && !scheduled) {
			jobService.deleteTasks(job.getJobId());
			jobService.deleteJobTaskDependencies(job.getJobId());
			jobService.delete(job.getJobId());
		} else {
			if (job.getState() == JobState.FAILED && !scheduled) {
				jobService.deleteJobTaskDependencies(job.getJobId());
			}
			jobService.update(job);
		}
	}

	/**
	 * This method will set the task state and send the task to updateTaskStatus to
	 * update the new state in database.
	 * 
	 * @param task  Task Object
	 * @param state New task state
	 */
	private void changeTaskStatus(Task task, TaskState state, boolean scheduled) {
		task.setState(state);
		updateTaskStatus(task, scheduled);
	}

	/**
	 * This method is used to add the task details to the database.
	 * 
	 * @param task Task object
	 */
	private void updateTaskStatus(Task task, boolean scheduled) {
		if (task.getState() == TaskState.EXECUTING) {
			if (scheduled) {
				Task tmp = jobService.readTask(task.getTaskId());
				if (tmp != null) {
					jobService.updateTask(task);
					return;
				}
			}
			jobService.insertTask(task);
			addTaskDependencies(task.getTasks(), task.getTaskId(), task.getJobId());
		} else {
			jobService.updateTask(task);
		}
	}

	private void addTaskDependencies(List<Task> tasks, String taskId, String jobId) {
		List<TaskDependencies> dependencies = tasks.stream()
				.map(sub -> new TaskDependencies(taskId, sub.getTaskId(), jobId)).collect(Collectors.toList());
		for (TaskDependencies dependency : dependencies) {
			jobService.insertTaskDependency(dependency);
		}
	}

	/**
	 * This method will create a random id for task and job.
	 * 
	 * @return String
	 */
	private synchronized String generateId() {
		try {
			Thread.sleep(2);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return String.valueOf(System.currentTimeMillis());
	}

	/**
	 * This method sets a new task id for each task and set the job id in each task.
	 * This method will be called recursively to set all the sub task with unique id
	 * and job id to each tasks.
	 * 
	 * @param tasks list of task
	 * @param jobId the job id for the task list
	 */
	private void setTaskUniqueIds(List<Task> tasks, String jobId) {
		for (Task task : tasks) {
			task.setJobId(jobId);
			task.setTaskId(generateId());
			setTaskUniqueIds(task.getTasks(), jobId);
		}
	}

	/**
	 * This method is used to get the default job scheduler.
	 * 
	 * @return JobScheduler is the default scheduler
	 */
	public static JobScheduler getDefaultScheduler() {
		return defaultScheduler;
	}

	/**
	 * This method is used to submit the job to start its execution, simple jobs
	 * will be started immediately and the jobs that has cron expression will be
	 * scheduled to be executed on the next execution time.
	 * 
	 * @param job
	 */
	public synchronized void submit(Job job) {
		job.setJobId(generateId());
		setTaskUniqueIds(job.getTasks(), job.getJobId());
		for (Task task : job.getTasks()) {
			findDependentTasks(task, task.getTasks());
		}
		scheduleJob(job, job.isScheduled());
	}

	/**
	 * This method will create the dependent tasks for all the sub tasks.
	 * 
	 * @param tasks
	 */
	private void findDependentTasks(Task task, List<Task> tasks) {
		taskDependencies.put(task.getTaskId(), new ArrayList<String>());
		for (Task tsk : tasks) {
			findDependentTasks(tsk, tsk.getTasks());
		}
		for (Task tsk : tasks) {
			taskDependencies.get(tsk.getTaskId()).add(task.getTaskId());
		}
	}

	public Map<String, String> getActiveJobStatus() {
		boolean entered = false;
		Map<String, String> data = null;
		try {
			entered = jobLock.tryLock();
			data = jobs.stream().collect(Collectors.toMap(Job::getJobId, job -> job.getState().name()));
		} finally {
			if (entered)
				jobLock.unlock();
		}
		return data;
	}

	public Map<String, String> getJobschedulerStatus() {
		Map<String, String> schedulerStatus = new HashMap<String, String>();
		schedulerStatus.put(defaultScheduler.schedulerName, defaultScheduler.daemon.isDone() ? "Stopped" : "Running");
		return schedulerStatus;
	}

	public static void main(String[] args) {
		// connect
		// 'jdbc:derby:/Users/arunalex/eclipse-workspace/job-scheduler/jobschedulerdb';
		String dbUrl = String.format("jdbc:derby:%s../../../jobschedulerdb;create=true",
				JobServiceImpl.class.getProtectionDomain().getCodeSource().getLocation().getPath().toString());
		try {
			Connection conn = DriverManager.getConnection(dbUrl);
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(
					"CREATE TABLE JOBS (ID varchar(50), STATE varchar(30), CLASS varchar(255), CRON varchar(30), PREVEXEC varchar(30), NEXTEXEC varchar(30), CREATEDON DATE, UPDATEDON DATE)");
			stmt.executeUpdate(
					"CREATE TABLE TASKS (ID varchar(50), JOBID varchar(50), STATE varchar(30), CLASS varchar(255), CREATEDON DATE, UPDATEDON DATE)");
			stmt.executeUpdate(
					"CREATE TABLE TASK_DEPENDENCIES (TASKID varchar(50), DEPENDENTID varchar(50), JOBID varchar(50))");
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			if (!e.getMessage().matches(".* already exists.*")) {
				e.printStackTrace();
			}
		}
		System.out.println("Finished");
		System.exit(0);
	}
}
