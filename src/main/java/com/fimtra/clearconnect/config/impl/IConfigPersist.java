/*
 * Copyright (c) 2016 Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.clearconnect.config.impl;

import java.util.Collection;

import com.fimtra.datafission.IRecord;

/**
 * This is the internal API for persistence of configuration records.
 * 
 * @author Paul Mackinlay
 */
public interface IConfigPersist {

	/**
	 * Saves the configRecord to a persistent store.
	 */
	void save(IRecord configRecord);

	/**
	 * Populates data from a persistent store into the configRecord. Existing data in the configRecord
	 * is replaced if it exists in the persistent store.
	 */
	void populate(IRecord configRecord);

	/**
	 * Returns a {@link Collection} of records name for records that have changed (created, updated
	 * or deleted) since the last time this method was called.
	 */
	Collection<String> getChangedRecordNames();
}
