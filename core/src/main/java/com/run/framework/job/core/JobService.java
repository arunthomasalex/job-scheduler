package com.run.framework.job.core;

import java.util.List;

/**
 * @author arunalex
 *
 */
public interface JobService {
	boolean insert(Job job);
	boolean insertTask(Task task);
	
	Job read(String jobId);
	Task readTask(String taskId);
	List<Task> readTasks(String jobId);
	
	boolean update(Job job);
	boolean updateTask(Task task);
	
	boolean delete(String jobId);
	boolean deleteTask(String taskId);
	boolean deleteTasks(String jobId);
}
