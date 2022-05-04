package com.run.framework.job.core;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.run.framework.job.constant.JobConstant;

public class JobConfiguration {

	private static JobConfiguration jobConfiguration = new JobConfiguration();
	
	private static final Properties configuration;
	private static URL configLoc;

	static {
		configuration = new Properties();
		try {
			configLoc = JobConfiguration.class.getClassLoader().getResource(JobConstant.JOB_CONFIG_FILE);
			configuration.load(new FileInputStream(configLoc.getPath()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static JobConfiguration getConfigurations() {
		return jobConfiguration;
	}
	
	public Object get(String key) {
		return JobConfiguration.configuration.get(key);
	}
	
	public String getProperty(String key) {
		return JobConfiguration.configuration.getProperty(key);
	}

	public String getProperty(String key, String defaultValue) {
		String value = JobConfiguration.configuration.getProperty(key);
		return value != null ? value : defaultValue;
	}
	
	public int size() {
		return JobConfiguration.configuration.size();
	}
	
	public synchronized boolean setProperty(String key, String value) {
		Object prevValue = JobConfiguration.configuration.setProperty(key, value);
		try {
			configuration.store(new FileOutputStream(configLoc.getPath()), "Job Configurations");
			return true;
		} catch (IOException e) {
			JobConfiguration.configuration.setProperty(key, prevValue.toString());
			return false;
		}
	}
	
	public Stream<Entry<Object, Object>> stream() {
		return StreamSupport.stream(new PropertySplitator(), false);
	}
	
	private class PropertySplitator implements Spliterator<Entry<Object, Object>> {
		private Iterator<Entry<Object, Object>> iterator = JobConfiguration.configuration.entrySet().iterator();
		private int size = JobConfiguration.configuration.size();
		@Override
		public boolean tryAdvance(Consumer<? super Entry<Object, Object>> action) {
			action.accept(iterator.next());
			return iterator.hasNext();
		}
		@Override
		public Spliterator<Entry<Object, Object>> trySplit() {
			return null;
		}
		@Override
		public long estimateSize() {
			return size;
		}
		@Override
		public int characteristics() {
			return Spliterator.DISTINCT;
		}
		
	}
}
