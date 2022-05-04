package com.run.framework.job.jmx;

import java.io.IOException;
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
import javax.management.ReflectionException;

import com.run.framework.job.constant.JobConstant;
import com.run.framework.job.core.JobConfiguration;
import com.run.framework.job.core.JobScheduler;

public class JobConfig implements DynamicMBean {
	private JobConfiguration configuration;

	public JobConfig() {
		configuration = JobConfiguration.getConfigurations();
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
		return "Success";
	}

	@Override
	public MBeanInfo getMBeanInfo() {
		Properties props = new Properties();
		try {
			props.load(JobScheduler.class.getClassLoader().getResourceAsStream(JobConstant.JOB_CONFIG_FILE));
		} catch (IOException e) {
			e.printStackTrace();
		}
		MBeanOperationInfo opInfo = new MBeanOperationInfo("test", "Run healthcheck", null, "boolean",
				MBeanOperationInfo.INFO);
		MBeanAttributeInfo[] attributeInfo = configuration.stream().map(conf -> {
			return new MBeanAttributeInfo(conf.getKey().toString(), "java.lang.String", "", true, true, false);
		}).collect(Collectors.toList()).toArray(MBeanAttributeInfo[]::new);
		return new MBeanInfo(JobScheduler.class.getCanonicalName(), "Fetch job related datas.", attributeInfo, null, new MBeanOperationInfo[] { opInfo }, null);
	}

}
