/*
 * Copyright (c) 2016 Ramon Servadei
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
package com.fimtra.ipcchannel;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.util.FileUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.ThreadUtils;

/**
 * An IPC service that can accept connections from {@link IpcChannel} instances.
 * 
 * @author Ramon Servadei
 */
public final class IpcService implements IEndPointService
{
    static final int SCAN_PERIOD_MILLIS = 5000;

    final Thread scanThread;
    final IReceiver clientReceiver;
    final EndPointAddress endPointAddress;
    final Map<String, ITransportChannel> clients;

    final String suffix;
    final File directory;

    boolean active;

    public IpcService(EndPointAddress endPointAddress, IReceiver receiver)
    {
        this.endPointAddress = endPointAddress;
        this.clientReceiver = receiver;

        // IPC file format:
        // client: UUID.port
        // server: port.UUID

        this.clients = new HashMap<String, ITransportChannel>();
        this.suffix = "." + this.endPointAddress.getPort();
        this.directory = new File(endPointAddress.getNode(), "" + endPointAddress.getPort());
        if (this.directory.exists())
        {
            try
            {
                FileUtils.deleteRecursive(this.directory);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not start IPC service; could not delete " + this.directory, e);
            }
        }
        if (!this.directory.exists() && !this.directory.mkdirs())
        {
            throw new RuntimeException("Could not start IPC service; could not create " + this.directory);
        }
        this.active = true;

        // todo ideally want to use Watchable (JDK7)
        this.scanThread = ThreadUtils.newDaemonThread(new Runnable()
        {
            @Override
            public void run()
            {
                scan();
            }
        }, "IpcService-" + endPointAddress);
        this.scanThread.start();

    }

    @Override
    public EndPointAddress getEndPointAddress()
    {
        return this.endPointAddress;
    }

    @Override
    public void destroy()
    {
        this.active = false;
        this.scanThread.interrupt();

        for (ITransportChannel client : this.clients.values())
        {
            client.destroy("IpcService shutting down");
        }
        this.clients.clear();
    }

    @Override
    public int broadcast(String messageContext, byte[] txMessage, ITransportChannel[] clients)
    {
        for (int i = 0; i < clients.length; i++)
        {
            try
            {
                clients[i].sendAsync(txMessage);
            }
            catch (Exception e)
            {
                Log.log(this, "Could no send broadcast message to ", ObjectUtils.safeToString(clients[i]));
            }
        }
        return clients.length;
    }

    @Override
    public void endBroadcast(String messageContext)
    {
        // noop for IPC
    }

    void scan()
    {
        final FileFilter fileFilter = new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                return pathname.getPath().endsWith(IpcService.this.suffix);
            }
        };

        while (this.active)
        {
            try
            {
                Thread.sleep(SCAN_PERIOD_MILLIS);
                
                for (File clientOutIpcFile : FileUtils.readFiles(this.directory, fileFilter))
                {
                    if (this.clients.containsKey(clientOutIpcFile))
                    {
                        continue;
                    }

                    String name = clientOutIpcFile.getName();
                    // client out: UUID.port
                    // server out: port.UUID
                    final String serviceOutIpcFile = new File(this.directory, this.endPointAddress.getPort() + "."
                        + name.substring(0, name.length() - this.suffix.length())).getAbsolutePath();

                    this.clients.put(name,
                        new IpcChannel(serviceOutIpcFile, clientOutIpcFile.getAbsolutePath(), new IReceiver()
                        {
                            @Override
                            public void onDataReceived(byte[] data, ITransportChannel source)
                            {
                                IpcService.this.clientReceiver.onDataReceived(data, source);
                            }

                            @Override
                            public void onChannelConnected(ITransportChannel tcpChannel)
                            {
                                IpcService.this.clientReceiver.onChannelConnected(tcpChannel);
                            }

                            @Override
                            public void onChannelClosed(ITransportChannel channel)
                            {
                                IpcService.this.clients.remove(channel);
                                IpcService.this.clientReceiver.onChannelClosed(channel);
                            }
                        }));
                }
            }
            catch (Exception e)
            {
                Log.log(IpcService.this, "Could not scan " + ObjectUtils.safeToString(IpcService.this.directory), e);
            }
        }
    }
}
