/*
 * Copyright (c) 2014 Ramon Servadei 
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
package com.fimtra.datafission;

/**
 * A validator is attached to an {@link IPublisherContext} and performs validation of record changes
 * 
 * @author Ramon Servadei
 */
public interface IValidator
{
    /**
     * Called when the validator is registered with a context. In general, code should call
     * {@link IPublisherContext#updateValidator(IValidator)} in this method to initialise validator
     * state.
     * 
     * @param context
     *            the context this validator is registering to
     */
    void onRegistration(IPublisherContext context);

    /**
     * Called when the validator is deregstered from a context.
     * 
     * @param context
     *            the context this validator is deregistering from
     */
    void onDeregistration(IPublisherContext context);

    /**
     * Validate a record
     * 
     * @param record
     *            the record to validate
     * @param change
     *            the change in the record
     */
    void validate(IRecord record, IRecordChange change);
}
