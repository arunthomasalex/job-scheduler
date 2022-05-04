package com.run.framework.job.util;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.run.framework.job.core.exception.JobScheduleException;

/**
 * @author arunalex
 *
 */
public final class JobUtil {

	/**
	 * This method calculates the execution time as per the quartz cron expression.
	 * 
	 * @param cronExpression This should be a valid quartz cron expression.
	 * @return long This returns the next execution time in milliseconds.
	 */
	public static long nextExecutionTime(String cronExpression) {
		Date now = new Date();
		CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
		Cron cron = parser.parse(cronExpression);
		ExecutionTime time = ExecutionTime.forCron(cron);
		ZonedDateTime dateTime = ZonedDateTime.ofInstant(now.toInstant(), ZoneId.systemDefault());
		dateTime = time.nextExecution(dateTime).orElse(null);
		if (dateTime != null) {
			return (dateTime.toInstant().toEpochMilli() - now.toInstant().toEpochMilli());
		} else {
			throw new JobScheduleException("Could not schedule the cron job.");
		}
	}
}
