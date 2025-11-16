/*
 * Copyright (c) 2013 Paul Mackinlay, Fimtra
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.clearconnect.config.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import com.fimtra.clearconnect.IPlatformServiceInstance;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Paul Mackinlay
 */
public class ConfigPublisherTest {

	private ConfigPublisher configPublisher;
	private IConfigPersist configPersist;
	private IPlatformServiceInstance platformServiceInstance;

	@Before
	public void setUp() {
		this.configPersist = mock(IConfigPersist.class);
		this.platformServiceInstance = mock(IPlatformServiceInstance.class);
		this.configPublisher = new ConfigPublisher(this.configPersist, this.platformServiceInstance);
	}

	@Test
	public void shouldNotPublishRecord() {
		this.configPublisher.run();
		verifyZeroInteractions(this.platformServiceInstance);
	}

	@Test
	public void shouldTryToPublishRecord() {
		String recordName = "test";
		when(this.configPersist.getChangedRecordNames()).thenReturn(Arrays.asList(recordName));
		this.configPublisher.run();
		verify(this.platformServiceInstance, times(1)).getOrCreateRecord(recordName);
	}

}
