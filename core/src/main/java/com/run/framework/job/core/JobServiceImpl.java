package com.run.framework.job.core;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author arunalex
 *
 */
public class JobServiceImpl implements JobService {
	private String dbUrl;

	public JobServiceImpl() {
		this(String.format("jdbc:derby:%s../../../jobschedulerdb",
				JobServiceImpl.class.getProtectionDomain().getCodeSource().getLocation().getPath().toString()));
	}

	public JobServiceImpl(String url) {
		this.dbUrl = url;
	}

	@Override
	public boolean insert(Job job) {
		String sql = "INSERT INTO JOBS (ID, CLASS, STATE, PREVEXEC, NEXTEXEC, CREATEDON, UPDATEDON) VALUES (?, ?, ?, ?, ?, ?, ?)";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, job.getJobId());
			statement.setString(2, job.getClass().getCanonicalName());
			statement.setString(3, job.getState().name());
			statement.setLong(4, job.getPrevExec());
			statement.setLong(5, job.getNextExec());
			statement.setDate(6, new Date(new java.util.Date().getTime()));
			statement.setDate(7, null);
			statement.execute();
			statement.closeOnCompletion();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean insertTask(Task task) {
		String sql = "INSERT INTO TASKS (ID, JOBID, CLASS, STATE, CREATEDON, UPDATEDON) VALUES (?, ?, ?, ?, ?, ?)";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, task.getTaskId());
			statement.setString(2, task.getJobId());
			statement.setString(3, task.getClass().getCanonicalName());
			statement.setString(4, task.getState().name());
			statement.setDate(5, new Date(new java.util.Date().getTime()));
			statement.setDate(6, null);
			statement.execute();
			statement.closeOnCompletion();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Job read(String jobId) {
		String sql = "SELECT CLASS, STATE, PREVEXEC, NEXTEXEC FROM JOBS WHERE ID = ?";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, jobId);
			if (statement.execute()) {
				ResultSet rs = statement.getResultSet();
				if (rs.next()) {
					String className = rs.getString("CLASS");
					Class<?> clazz = Class.forName(className);
					Job job = (Job) clazz.getConstructor().newInstance();
					job.setJobId(jobId);
					job.setState(JobState.valueOf(rs.getString("STATE")));
					job.setPrevExec(rs.getLong("PREVEXEC"));
					job.setNextExec(rs.getLong("NEXTEXEC"));
					return job;
				}
			}
		} catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException
				| IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Task readTask(String taskId) {
		String sql = "SELECT JOBID, CLASS, STATE FROM TASKS WHERE ID = ?";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, taskId);
			if (statement.execute()) {
				ResultSet rs = statement.getResultSet();
				if (rs.next()) {
					String className = rs.getString("CLASS");
					Class<?> clazz = ClassLoader.getSystemClassLoader().loadClass(className);
					Task task = (Task) clazz.getConstructor().newInstance();
					task.setTaskId(taskId);
					task.setJobId(rs.getString("JOBID"));
					task.setState(TaskState.valueOf(rs.getString("STATE")));
					return task;
				}
			}
		} catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException
				| IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<Task> readTasks(String jobId) {
		String sql = "SELECT ID, CLASS, STATE FROM TASKS WHERE JOBID = ?";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, jobId);
			if (statement.execute()) {
				List<Task> tasks = new ArrayList<Task>();
				ResultSet rs = statement.getResultSet();
				while (rs.next()) {
					String className = rs.getString("CLASS");
					Class<?> clazz = Class.forName(className);
					Task task = (Task) clazz.getConstructor().newInstance();
					task.setTaskId(rs.getString("ID"));
					task.setJobId(jobId);
					task.setState(TaskState.valueOf(rs.getString("STATE")));
					tasks.add(task);
				}

				return tasks;
			}
		} catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException
				| IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean update(Job job) {
		String sql = "UPDATE JOBS SET STATE = ?, PREVEXEC = ?, NEXTEXEC = ?, UPDATEDON = ? WHERE ID = ?";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, job.getState().name());
			statement.setLong(2, job.getPrevExec());
			statement.setLong(3, job.getNextExec());
			statement.setDate(4, new Date(new java.util.Date().getTime()));
			statement.setString(5, job.getJobId());
			statement.execute();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean updateTask(Task task) {
		String sql = "UPDATE TASKS SET STATE = ?, UPDATEDON = ? WHERE ID = ?";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, task.getState().name());
			statement.setDate(2, new Date(new java.util.Date().getTime()));
			statement.setString(3, task.getTaskId());
			statement.execute();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean delete(String jobId) {
		String sql = "DELETE FROM JOBS WHERE ID = ?";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, jobId);
			statement.execute();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean deleteTask(String taskId) {
		String sql = "DELETE FROM TASKS WHERE ID = ?";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, taskId);
			statement.execute();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean deleteTasks(String jobId) {
		String sql = "DELETE FROM TASKS WHERE JOBID = ?";
		try (Connection connection = DriverManager.getConnection(dbUrl);
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, jobId);
			statement.execute();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

}
