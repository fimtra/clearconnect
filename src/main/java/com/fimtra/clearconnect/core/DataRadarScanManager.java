/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
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
package com.fimtra.clearconnect.core;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import com.fimtra.clearconnect.expression.ExpressionProcessor;
import com.fimtra.datafission.IPublisherContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.datafission.IValidator;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * A data radar scan manager exists in a {@link PlatformServiceInstance} and manages the scanning of
 * all record changes for matching data signatures setup by RPC calls that register
 * {@link DataRadarSpecification} instances.
 * <p>
 * The {@link DataRadarSpecification} instances are setup by RPC calls from the {@link RadarStation}
 * , which in-turn receives the initiating registration RPC from an agent.
 * <p>
 * Each {@link DataRadarSpecification} is hosted in a {@link DataRadarScan} and has an associated
 * data radar record. When records change, the fields are scanned for data signature matches; any
 * matches cause the DataRadarScan to re-evaluate and if the record matches all data signatures then
 * it appears on the data radar (the record name is added as a field to the corresponding data radar
 * record).
 * 
 * @author Ramon Servadei
 */
public final class DataRadarScanManager
{
    public static final String RPC_REGISTER_DATA_RADAR = "registerDataRadar";
    public static final String RPC_DEREGISTER_DATA_RADAR = "deregisterDataRadar";
    public static final String RADAR_STATION = "RadarStation";
    public static final String RADAR_SCAN_RECORD_PREFIX = "RadarScan:";

    public static final String RPC_REGISTER_DATA_RADAR_SPECIFICATION = "registerDataRadarSpecification";
    public static final String RPC_DEREGISTER_DATA_RADAR_SPECIFICATION = "deregisterDataRadarSpecification";

    static void scanForSignatureMatch(IRecord record, IRecordChange atomicChange,
        ConcurrentMap<String, Set<DataRadarScan>> dataRadarScansPerField, Object scanLock)
    {

        // 1. build up the set of data radar scans that are affected by any field changes
        // 2. process the record against the data radar scans (they are stateless)

        // #1 get the set of radar scans to process
        final Set<DataRadarScan> radarsScansToProcess = new HashSet<DataRadarScan>();
        Map<String, IValue> entries = atomicChange.getPutEntries();
        Map.Entry<String, IValue> entry = null;
        String fieldName = null;
        Set<DataRadarScan> dataRadarScans = null;
        if (entries.size() > 0)
        {
            for (Iterator<Map.Entry<String, IValue>> it = entries.entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                fieldName = entry.getKey();
                dataRadarScans = dataRadarScansPerField.get(fieldName);
                if (dataRadarScans != null && dataRadarScans.size() > 0)
                {
                    radarsScansToProcess.addAll(dataRadarScans);
                }
            }
        }

        // process removed fields
        entries = atomicChange.getRemovedEntries();
        if (entries.size() > 0)
        {
            for (Iterator<Map.Entry<String, IValue>> it = entries.entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                fieldName = entry.getKey();
                dataRadarScans = dataRadarScansPerField.get(fieldName);
                if (dataRadarScans != null && dataRadarScans.size() > 0)
                {
                    radarsScansToProcess.addAll(dataRadarScans);
                }
            }
        }

        if (radarsScansToProcess.size() > 0)
        {
            // #3 scan the record using the discovered radar scans
            final Map<String, IValue> flattenedMap = record.asFlattenedMap();

            // todo This will slow down record update throughput across multiple threads...
            synchronized (scanLock)
            {
                // START SCAN
                for (DataRadarScan dataRadarScan : radarsScansToProcess)
                {
                    dataRadarScan.beginScan();
                }

                // SCAN
                IValue value = null;
                for (Iterator<Map.Entry<String, IValue>> it = flattenedMap.entrySet().iterator(); it.hasNext();)
                {
                    entry = it.next();
                    fieldName = entry.getKey();
                    value = entry.getValue();

                    for (DataRadarScan dataRadarScan : radarsScansToProcess)
                    {
                        dataRadarScan.scanRecordField(fieldName, value);
                    }
                }

                // END SCAN
                for (DataRadarScan dataRadarScan : radarsScansToProcess)
                {
                    dataRadarScan.endScan(record.getName());
                }
            }
        }
    }

    /**
     * A data radar scan performs the job of scanning record changes and matching them against the
     * data signatures of a {@link DataRadarSpecification}. The data radar scan has a data radar
     * record that publishes the results of the scan. Records that match all signatures of the radar
     * specification will appear on the data radar record.
     * <p>
     * <b>NOT THREAD SAFE</b>
     * 
     * @author Ramon Servadei
     */
    final class DataRadarScan
    {
        final ExpressionProcessor processor;
        final IRecord radarRecord;
        final DataRadarSpecification dataRadarSpecification;

        DataRadarScan(DataRadarSpecification dataRadarSpecification)
        {
            this.processor = new ExpressionProcessor(dataRadarSpecification.dataRadarSignatureExpression);
            this.dataRadarSpecification = dataRadarSpecification;
            this.radarRecord =
                DataRadarScanManager.this.service.getOrCreateRecord(this.dataRadarSpecification.getName());
        }

        void beginScan()
        {
            this.processor.beginScan();
        }

        void scanRecordField(String fieldName, IValue value)
        {
            this.processor.processRecordField(fieldName, value);
        }

        void endScan(String recordName)
        {
            boolean publish = false;
            // now evaluate the radar expression
            if (this.processor.evalate())
            {
                publish = this.radarRecord.put(recordName, DataRadarScanManager.this.serviceInstanceId) == null;
            }
            else
            {
                publish = this.radarRecord.remove(recordName) != null;
            }
            if (publish)
            {
                DataRadarScanManager.this.service.publishRecord(this.radarRecord);
            }
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + " [" + this.radarRecord + ", " + this.dataRadarSpecification + "]";
        }

        /**
         * @return the list of fields that form the data signature scan for the radar
         */
        List<String> getScannedFields()
        {
            return this.processor.getScannedFields();
        }

        void destroy()
        {
            DataRadarScanManager.this.service.deleteRecord(this.radarRecord);
            this.processor.destroy();
        }

    }

    final ConcurrentMap<String, DataRadarScan> dataRadarScans;
    final ConcurrentMap<String, Set<DataRadarScan>> dataRadarScansPerField;
    final PlatformServiceInstance service;
    final String serviceInstanceId;
    final IValidator validator;

    DataRadarScanManager(PlatformServiceInstance service)
    {
        super();
        this.service = service;
        this.serviceInstanceId =
            PlatformUtils.composePlatformServiceInstanceID(service.getPlatformServiceFamily(),
                service.getPlatformServiceMemberName());
        this.dataRadarScans = new ConcurrentHashMap<String, DataRadarScanManager.DataRadarScan>();
        this.dataRadarScansPerField = new ConcurrentHashMap<String, Set<DataRadarScan>>();

        this.validator = new IValidator()
        {
            @Override
            public void validate(IRecord record, IRecordChange change)
            {
                if (record.getName().startsWith(DataRadarScanManager.RADAR_SCAN_RECORD_PREFIX)
                    || ContextUtils.isSystemRecordName(record.getName()))
                {
                    return;
                }

                scanForSignatureMatch(record, change, DataRadarScanManager.this.dataRadarScansPerField, this);
            }

            @Override
            public void onRegistration(IPublisherContext context)
            {
            }

            @Override
            public void onDeregistration(IPublisherContext context)
            {
            }
        };

        this.service.addValidator(this.validator);

        final RpcInstance registerDataSignatureRpc =
            new RpcInstance(TypeEnum.TEXT, RPC_REGISTER_DATA_RADAR_SPECIFICATION,
                new String[] { "dataRadarSpecification" }, TypeEnum.TEXT);
        registerDataSignatureRpc.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                DataRadarSpecification dataRadarSpecification =
                    DataRadarSpecification.fromWireString(args[0].textValue());
                registerDataRadar(dataRadarSpecification);
                return PlatformUtils.OK;
            }
        });
        if (!service.publishRPC(registerDataSignatureRpc))
        {
            throw new RuntimeException("Could not create RPC " + RPC_REGISTER_DATA_RADAR_SPECIFICATION);
        }

        final RpcInstance deregisterDataSignatureRpc =
            new RpcInstance(TypeEnum.TEXT, RPC_DEREGISTER_DATA_RADAR_SPECIFICATION,
                new String[] { "dataRadarSpecification" }, TypeEnum.TEXT);
        deregisterDataSignatureRpc.setHandler(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                DataRadarSpecification dataRadarSpecification =
                    DataRadarSpecification.fromWireString(args[0].textValue());
                deregisterDataRadar(dataRadarSpecification);
                return PlatformUtils.OK;
            }
        });
        if (!service.publishRPC(deregisterDataSignatureRpc))
        {
            throw new RuntimeException("Could not create RPC " + RPC_DEREGISTER_DATA_RADAR_SPECIFICATION);
        }

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
    }

    void destroy()
    {
        if (this.service.removeValidator(this.validator))
        {
            Log.log(this, "Destroying ", ObjectUtils.safeToString(this));
            clearDataRadarScans();
        }
    }

    void clearDataRadarScans()
    {
        for (Iterator<Map.Entry<String, DataRadarScanManager.DataRadarScan>> it =
            this.dataRadarScans.entrySet().iterator(); it.hasNext();)
        {
            removeAndDestroyDataRadarScan(it.next().getKey());
        }
    }

    void registerDataRadar(DataRadarSpecification dataRadarSpecification)
    {
        if (this.dataRadarScans.containsKey(dataRadarSpecification.getName()))
        {
            Log.log(this, this.serviceInstanceId, " ignoring duplicate registration for ",
                ObjectUtils.safeToString(dataRadarSpecification));
            return;
        }

        final DataRadarScan dataRadarScan = new DataRadarScan(dataRadarSpecification);
        this.dataRadarScans.put(dataRadarSpecification.getName(), dataRadarScan);

        Log.log(this, this.serviceInstanceId, " registering: ", ObjectUtils.safeToString(dataRadarSpecification));

        final ConcurrentMap<String, Set<DataRadarScan>> addedSignature =
            new ConcurrentHashMap<String, Set<DataRadarScan>>();

        // now index the data radar scan per field
        for (String field : dataRadarScan.getScannedFields())
        {
            Set<DataRadarScan> dataRadarScans = this.dataRadarScansPerField.get(field);
            if (dataRadarScans == null)
            {
                dataRadarScans = new CopyOnWriteArraySet<DataRadarScan>();
                this.dataRadarScansPerField.put(field, dataRadarScans);
            }
            dataRadarScans.add(dataRadarScan);

            final Set<DataRadarScan> set = new HashSet<DataRadarScan>();
            set.add(dataRadarScan);
            addedSignature.put(field, set);
        }

        // pass all existing records through the new data radar scan to get it up-to-speed
        this.service.updateValidator(new IValidator()
        {
            @Override
            public void validate(IRecord record, IRecordChange change)
            {
                scanForSignatureMatch(record, change, addedSignature, this);
            }

            @Override
            public void onRegistration(IPublisherContext context)
            {
            }

            @Override
            public void onDeregistration(IPublisherContext context)
            {
            }
        });
    }

    void deregisterDataRadar(DataRadarSpecification dataRadarSpecification)
    {
        Log.log(this, this.serviceInstanceId, " deregistering: ", ObjectUtils.safeToString(dataRadarSpecification));
        removeAndDestroyDataRadarScan(dataRadarSpecification.getName());
    }

    void removeAndDestroyDataRadarScan(final String dataRadarScanName)
    {
        try
        {
            final DataRadarScan removeDataRadarScan = this.dataRadarScans.remove(dataRadarScanName);
            if (removeDataRadarScan != null)
            {
                Log.log(this, this.serviceInstanceId, " remove and destroy ",
                    ObjectUtils.safeToString(removeDataRadarScan));
                // remove fields pointing to the radar scan
                Map.Entry<String, Set<DataRadarScan>> entry = null;
                Set<DataRadarScan> value = null;
                for (Iterator<Map.Entry<String, Set<DataRadarScan>>> it =
                    this.dataRadarScansPerField.entrySet().iterator(); it.hasNext();)
                {
                    entry = it.next();
                    value = entry.getValue();
                    value.remove(removeDataRadarScan);
                    if (value.size() == 0)
                    {
                        it.remove();
                    }
                }

                // remove the radar record
                removeDataRadarScan.destroy();
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not destroy data radar: " + dataRadarScanName, e);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [" + this.service + "]";
    }
}
