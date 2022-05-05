package com.run.framework.job.jmx;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;

import com.run.framework.job.constant.JobConstant;
import com.run.framework.job.core.JobConfiguration;
import com.run.framework.job.core.JobScheduler;

public class JobConfig implements DynamicMBean {
	private JobConfiguration configuration;
	private JobScheduler scheduler;
	private static final String JOB_SCHEDULER_ACTION_NAME = "jobSchedulerAction";

	public JobConfig() {
		configuration = JobConfiguration.getConfigurations();
		scheduler = JobScheduler.getDefaultScheduler();
	}

	@Override
	public Object getAttribute(String attribute)
			throws AttributeNotFoundException, MBeanException, ReflectionException {
		return configuration.get(attribute);
	}

	@Override
	public void setAttribute(Attribute attribute)
			throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
		configuration.setProperty(attribute.getName(), attribute.getValue().toString());
	}

	@Override
	public AttributeList getAttributes(String[] attributes) {
		return Stream.of(attributes).map(attr -> new Attribute(attr, configuration.get(attr)))
				.collect(Collectors.toCollection(AttributeList::new));
	}

	@Override
	public AttributeList setAttributes(AttributeList attributes) {
		attributes.asList().forEach(attr -> configuration.setProperty(attr.getName(), attr.getValue().toString()));
		return attributes;
	}

	@Override
	public Object invoke(String actionName, Object[] params, String[] signature)
			throws MBeanException, ReflectionException {
		if (actionName == null) {
			throw new RuntimeOperationsException(new IllegalArgumentException("Operation name cannot be null"),
					"Cannot invoke a null operation in " + scheduler.getClass().getName());
		}
		if (actionName.equals(JobConfig.JOB_SCHEDULER_ACTION_NAME)) {
			Optional<String> action = Arrays.stream(params).map(Object::toString)
					.filter(param -> !param.equals("String")).findFirst();
			if (action.isPresent())
				switch (action.get()) {
				case "start":
					scheduler.startScheduler();
					return "Started job scheduler.";
				case "stop":
					scheduler.stopScheduler();
					return "Stopped job scheduler.";
				case "forceStop":
					scheduler.forceStopScheduler();
					return "Stopped job scheduler.";
				default:
					return "Invalid action.";
				}
		}
		return "Invalid method.";
	}

	@Override
	public MBeanInfo getMBeanInfo() {
		Properties props = new Properties();
		try {
			props.load(JobScheduler.class.getClassLoader().getResourceAsStream(JobConstant.JOB_CONFIG_FILE));
		} catch (IOException e) {
			e.printStackTrace();
		}

		MBeanParameterInfo startInfo = new MBeanParameterInfo("start", "java.lang.String", "Start job scheduler");
		MBeanParameterInfo stopInfo = new MBeanParameterInfo("stop", "java.lang.String", "Stop job scheduler");
		MBeanParameterInfo forceStopInfo = new MBeanParameterInfo("forcestop", "java.lang.String",
				"Stop job scheduler");
		MBeanOperationInfo schedulerAction = new MBeanOperationInfo(JobConfig.JOB_SCHEDULER_ACTION_NAME,
				"Start default jod scheduler.", new MBeanParameterInfo[] { startInfo, stopInfo, forceStopInfo },
				"java.lang.String", MBeanOperationInfo.ACTION_INFO);
		MBeanAttributeInfo[] attributeInfo = configuration.stream().map(conf -> {
			return new MBeanAttributeInfo(conf.getKey().toString(), "java.lang.String", "", true, true, false);
		}).collect(Collectors.toList()).toArray(MBeanAttributeInfo[]::new);
		return new MBeanInfo(JobScheduler.class.getCanonicalName(), "Fetch job related datas.", attributeInfo, null,
				new MBeanOperationInfo[] { schedulerAction }, null);
	}

}
