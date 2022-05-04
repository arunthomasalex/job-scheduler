package com.run.framework.job.core.exception;

/**
 * @author arunalex
 *
 */
public class JobDatabaseException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4706377712656392093L;
	
	public JobDatabaseException() {
		super();
	}
	
	public JobDatabaseException(String msg) {
		super(msg);
	}
	
	public JobDatabaseException(Throwable throwable) {
		super(throwable);
	}
	
	public JobDatabaseException(String msg, Throwable throwable) {
		super(msg, throwable);
	}
}
