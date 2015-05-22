/*
 * Copyright (c) 2013 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.config.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.platform.IPlatformServiceInstance;
import com.fimtra.platform.config.impl.ConfigDirReader;
import com.fimtra.platform.config.impl.ConfigPublisher;

/**
 * @author Paul Mackinlay
 */
public class ConfigPublisherTest {

	private ConfigPublisher configPublisher;
	private ConfigDirReader configDirReader;
	private IPlatformServiceInstance platformServiceInstance;

	@Before
	public void setUp() {
		this.configDirReader = mock(ConfigDirReader.class);
		this.platformServiceInstance = mock(IPlatformServiceInstance.class);
		this.configPublisher = new ConfigPublisher(this.configDirReader, this.platformServiceInstance);
	}

	@Test
	public void shouldNotPublishRecord() {
		this.configPublisher.run();
		verifyZeroInteractions(this.platformServiceInstance);
	}

	@Test
	public void shouldTryToPublishRecord() {
		String recordName = "test";
		when(this.configDirReader.updateRecordFileCache()).thenReturn(Arrays.asList(new File(recordName + ".record")));
		this.configPublisher.run();
		verify(this.platformServiceInstance, times(1)).getOrCreateRecord(recordName);
	}

}
