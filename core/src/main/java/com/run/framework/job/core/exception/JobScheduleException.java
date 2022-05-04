package com.run.framework.job.core.exception;

/**
 * @author arunalex
 *
 */
public class JobScheduleException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7220153340145744354L;

	public JobScheduleException() {
		super();
	}
	
	public JobScheduleException(String msg) {
		super(msg);
	}
	
	public JobScheduleException(Throwable throwable) {
		super(throwable);
	}
	
	public JobScheduleException(String msg, Throwable throwable) {
		super(msg, throwable);
	}
}
