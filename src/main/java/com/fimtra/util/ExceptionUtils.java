/*
 * Copyright (c) 2013 Paul Mackinlay 
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
package com.fimtra.util;


/**
 * Utility methods for exceptions.
 *
 * @author Paul Mackinlay
 */
public abstract class ExceptionUtils {

	private ExceptionUtils() {
		// Not for instantiation
	}

	/**
	 *Gets the stack of the {@link Throwable}, cascading through all causes.
	 */
	public static final String getFullStackTrace(Throwable throwable) {
		StringBuilder stackBuilder = getStackBuilder(throwable);
		return stackBuilder.toString();
	}

	private static StringBuilder getStackBuilder(Throwable throwable) {
		StringBuilder stackBuilder = new StringBuilder();
		stackBuilder.append(throwable.getClass().getCanonicalName()).append(": ").append(throwable.getMessage()).append(SystemUtils.lineSeparator());
		for (StackTraceElement e : throwable.getStackTrace()) {
			stackBuilder.append("\t").append("at ").append(e.toString()).append(SystemUtils.lineSeparator());
		}
		if (throwable.getCause() != null) {
			stackBuilder.append("Caused by ").append(getStackBuilder(throwable.getCause()));
		}
		return stackBuilder;
	}
}
