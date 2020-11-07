/*
 * Copyright (c) 2013 Ramon Servadei, Paul Mackinlay, Fimtra
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.clearconnect.core;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.border.CompoundBorder;
import javax.swing.event.InternalFrameAdapter;
import javax.swing.event.InternalFrameEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;

import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.core.PlatformRegistry.IPlatformSummaryRecordFields;
import com.fimtra.clearconnect.core.PlatformRegistry.IRegistryRecordNames;
import com.fimtra.clearconnect.event.EventListenerUtils;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames.IContextConnectionsRecordFields;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.Context;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.IStatusAttribute;
import com.fimtra.datafission.core.IStatusAttribute.Connection;
import com.fimtra.datafission.core.ProxyContext;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.field.BlobValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.datafission.ui.ColumnOrientedRecordTable;
import com.fimtra.datafission.ui.ColumnOrientedRecordTableModel;
import com.fimtra.datafission.ui.RowOrientedRecordTable;
import com.fimtra.datafission.ui.RowOrientedRecordTableModel;
import com.fimtra.lf.FimtraTableHeaderUI;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.thimble.ISequentialRunnable;
import com.fimtra.util.FileUtils;
import com.fimtra.util.Log;
import com.fimtra.util.ThreadUtils;
import com.fimtra.util.is;

/**
 * Connects to a {@link PlatformRegistry} and provides a graphical view onto the
 * services/records/rpcs on the platform.
 *
 * @author Ramon Servadei
 * @author Paul Mackinlay
 */
public class PlatformDesktop
{
    static final Executor executor =
            ThreadUtils.newSingleThreadExecutorService(ThreadUtils.getDirectCallingClassSimpleName());
    public static final String _SEP_ = " : ";

    static Executor getExecutor()
    {
        return executor;
    }

    static abstract class AbstractPlatformDesktopView
    {
        final static int indexClass = 0;
        final static int indexX = 1;
        final static int indexY = 2;
        final static int indexWidth = 3;
        final static int indexHeight = 4;
        final static int indexTitle = 5;
        final static int indexViewKey = 6;
        final static int indexViewType = 7;

        final static int indexCustomStart = 8;

        static AbstractPlatformDesktopView fromStateString(PlatformDesktop desktop, String stateString)
        {
            final String[] tokens = stateString.split(",");

            String viewKey = tokens[indexViewKey];
            viewKey = "null".equals(viewKey.toLowerCase()) ? null : viewKey;

            AbstractPlatformDesktopView view = null;
            if (RecordSubscriptionView.class.getSimpleName().equals(tokens[indexClass]))
            {
                view = new RecordSubscriptionView(desktop,
                        PlatformMetaDataViewEnum.valueOf(tokens[indexViewType]), viewKey);
            }
            else if (MetaDataView.class.getSimpleName().equals(tokens[indexClass]))
            {
                view = new MetaDataView(desktop, PlatformMetaDataViewEnum.valueOf(tokens[indexViewType]),
                        viewKey);
            }
            else if (RpcView.class.getSimpleName().equals(tokens[indexClass]))
            {
                view = new RpcView(desktop, PlatformMetaDataViewEnum.valueOf(tokens[indexViewType]), viewKey);
            }

            if (view == null)
            {
                return null;
            }

            try
            {
                view.getFrame().setLocation(
                        new Point(Integer.parseInt(tokens[indexX]), Integer.parseInt(tokens[indexY])));
                view.getFrame().setSize(Integer.parseInt(tokens[indexWidth]),
                        Integer.parseInt(tokens[indexHeight]));
            }
            catch (Exception e)
            {
                Log.log(PlatformDesktop.class, "Could not set position or size for '" + stateString + "'", e);
            }

            view.resolveFromStateString(tokens);

            return view;
        }

        static String getTableStateString(String[] tokens, int start)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = start; i < tokens.length; i++)
            {
                if (i > start)
                {
                    sb.append(",");
                }
                sb.append(tokens[i]);
            }
            return sb.toString();
        }

        final PlatformDesktop desktop;
        final JInternalFrame frame;
        final String metaDataViewKey;
        final PlatformMetaDataViewEnum metaDataViewType;
        final String title;
        final IObserverContext context;
        final String sessionId;

        AbstractPlatformDesktopView(String title, PlatformDesktop desktop,
                PlatformMetaDataViewEnum metaDataViewType, String metaDataViewKey)
        {
            super();
            this.title = title;
            this.desktop = desktop;
            this.context = metaDataViewType.getContextForMetaDataViewType(desktop.getMetaDataModel(),
                    metaDataViewKey);

            // get session AFTER realising the context
            switch(metaDataViewType)
            {
                case INSTANCE_RPC:
                case INSTANCE_RECORD:
                case RPCS_PER_INSTANCE:
                case RECORDS_PER_INSTANCE:
                case CLIENTS_PER_INSTANCE:
                    this.sessionId = desktop.getMetaDataModel().getSessionIdForPlatformServiceInstance(
                            metaDataViewKey);
                    break;
                case RPC:
                case RECORD:
                case RPCS_PER_SERVICE:
                case RECORDS_PER_SERVICE:
                case CLIENTS_PER_SERVICE:
                    this.sessionId =
                            desktop.getMetaDataModel().getSessionIdForPlatformService(metaDataViewKey);
                    break;
                default:
                    this.sessionId = null;
            }

            this.frame = new JInternalFrame(title, true, true, true, true);
            this.metaDataViewKey = metaDataViewKey;
            this.metaDataViewType = metaDataViewType;
            int w = 400;
            int h = 200;
            this.frame.setSize(w, h);
            final Point p = desktop.desktopPane.getMousePosition();
            if (p != null)
            {
                this.frame.setLocation((int) p.getX() - 30, (int) p.getY() - 5);
            }
            this.frame.setVisible(true);
            this.frame.addInternalFrameListener(new InternalFrameAdapter()
            {
                @Override
                public void internalFrameClosed(InternalFrameEvent e)
                {
                    super.internalFrameClosed(e);
                    AbstractPlatformDesktopView.this.desktop.getViews().remove(
                            AbstractPlatformDesktopView.this);
                    AbstractPlatformDesktopView.this.frame.removeInternalFrameListener(this);
                    AbstractPlatformDesktopView.this.frame.getContentPane().removeAll();
                    destroy();
                }
            });

            this.frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
            this.desktop.getViews().add(this);
            this.desktop.getDesktopPane().add(getFrame());
            getFrame().toFront();
        }

        final String toStateString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(getClass().getSimpleName()).append(","); // 0
            sb.append(getFrame().getX()).append(","); // 1
            sb.append(getFrame().getY()).append(","); // 2
            sb.append(getFrame().getWidth()).append(","); // 3
            sb.append(getFrame().getHeight()).append(","); // 4
            sb.append(this.metaDataViewType).append(","); // 5
            sb.append(this.metaDataViewKey).append(","); // 6
            sb.append(this.metaDataViewType).append(","); // 7
            toResolvableState(sb);
            return sb.toString();
        }

        abstract void toResolvableState(StringBuilder sb);

        abstract void resolveFromStateString(String[] tokens);

        JInternalFrame getFrame()
        {
            return this.frame;
        }

        abstract void destroy();

        IRecord getSelectedRecord()
        {
            throw new IllegalStateException();
        }
    }

    static class TableStatusListener implements IRecordListener
    {
        final JTable table;

        TableStatusListener(JTable table)
        {
            this.table = table;
        }

        @Override
        public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
        {
            final Connection status =
                    IStatusAttribute.Utils.getStatus(Connection.class, imageValidInCallingThreadOnly);
            if (status != null)
            {
                switch(status)
                {
                    case CONNECTED:
                        this.table.setBackground(null);
                        break;
                    case DISCONNECTED:
                        this.table.setBackground(Color.orange);
                        break;
                    case RECONNECTING:
                        this.table.setBackground(Color.yellow);
                        break;
                }
            }
        }
    }

    static class RecordSubscriptionView extends AbstractPlatformDesktopView
    {
        private final static int indexSubscriptions = indexCustomStart;

        private static String getSubscriptionsStateString(RecordSubscriptionView view)
        {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (String string : view.subscribedRecords)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.append("|");
                }
                sb.append(string.replace("|", "\\|"));
            }
            return sb.toString();
        }

        private static List<String> getSubscriptionsFromStateString(String state)
        {
            final String[] tokens = state.split("\\|");
            List<String> subscriptions = new ArrayList<>(tokens.length);
            for (String string : tokens)
            {
                subscriptions.add(string.replace("\\|", "|"));
            }
            return subscriptions;
        }

        final IRecordListener statusObserver;
        final ColumnOrientedRecordTable table;
        final ColumnOrientedRecordTableModel model;
        final Set<String> subscribedRecords;
        final TableSummaryPanel tableSummaryPanel;

        public RecordSubscriptionView(PlatformDesktop desktop, PlatformMetaDataViewEnum metaDataViewType,
                String metaDataViewKey)
        {
            super("RecordSubscriptions" + _SEP_ + metaDataViewKey, desktop, metaDataViewType,
                    metaDataViewKey);

            this.model = new ColumnOrientedRecordTableModel();
            this.table = new ColumnOrientedRecordTable(this.model);
            this.statusObserver = new TableStatusListener(this.table);

            this.context.addObserver(this.sessionId, this.statusObserver,
                    IObserverContext.ISystemRecordNames.CONTEXT_STATUS);

            this.subscribedRecords = Collections.synchronizedSet(new HashSet<>());
            this.model.addRecordRemovedListener(this.context);

            prepareTablePopupMenu();

            this.frame.add(new JScrollPane(this.table));
            this.tableSummaryPanel = new TableSummaryPanel(this.table.getModel());
            this.frame.add(this.tableSummaryPanel, BorderLayout.SOUTH);

            synchronized (PlatformMetaDataViewEnum.recordSubscriptionViews)
            {
                PlatformMetaDataViewEnum.recordSubscriptionViews.put(this.metaDataViewKey, this);
            }
        }

        @Override
        void toResolvableState(StringBuilder sb)
        {
            sb.append(getSubscriptionsStateString(this)).append(","); // 8
            sb.append(this.table.toStateString()); // the rest
        }

        @Override
        void resolveFromStateString(String[] tokens)
        {
            for (String recordSubscription : getSubscriptionsFromStateString(tokens[indexSubscriptions]))
            {
                subscribeFor(recordSubscription);
            }

            this.table.fromStateString(getTableStateString(tokens, indexSubscriptions + 1));
        }

        private void prepareTablePopupMenu()
        {
            JPopupMenu popupMenu = new JPopupMenu();
            JMenuItem menuItem = new JMenuItem();
            menuItem.setAction(new AbstractAction()
            {
                private static final long serialVersionUID = 1L;

                @Override
                public void actionPerformed(ActionEvent e)
                {
                    final IRecord selectedRecord = RecordSubscriptionView.this.table.getSelectedRecord();
                    unsubscribeFor(selectedRecord.getName());
                    RecordSubscriptionView.this.table.getModel().recordUnsubscribed(selectedRecord);
                }
            });
            menuItem.setText("Remove");
            popupMenu.add(menuItem);
            this.table.setComponentPopupMenu(popupMenu);
        }

        @Override
        protected void destroy()
        {
            // if the constructor throws an exception, this can be null
            if (this.context != null)
            {
                this.tableSummaryPanel.destroy();
                this.table.setComponentPopupMenu(null);
                synchronized (PlatformMetaDataViewEnum.recordSubscriptionViews)
                {
                    PlatformMetaDataViewEnum.recordSubscriptionViews.remove(this.metaDataViewKey);
                }

                this.context.executeSequentialCoreTask(new ISequentialRunnable()
                {
                    @Override
                    public Object context()
                    {
                        return RecordSubscriptionView.this;
                    }

                    @Override
                    public void run()
                    {
                        RecordSubscriptionView.this.context.removeObserver(
                                RecordSubscriptionView.this.statusObserver,
                                ISystemRecordNames.CONTEXT_STATUS);
                        RecordSubscriptionView.this.context.removeObserver(RecordSubscriptionView.this.model,
                                RecordSubscriptionView.this.subscribedRecords.toArray(new String[0]));
                        RecordSubscriptionView.this.model.removeRecordRemovedListener(
                                RecordSubscriptionView.this.context);
                        PlatformMetaDataViewEnum.deregister(RecordSubscriptionView.this.context,
                                RecordSubscriptionView.this.model);
                    }
                });
            }
        }

        void subscribeFor(String recordNameToSubscribe)
        {
            if (this.subscribedRecords.add(recordNameToSubscribe))
            {
                this.context.addObserver(this.sessionId, this.model, recordNameToSubscribe);
            }
        }

        void unsubscribeFor(String recordNameToSubscribe)
        {
            if (this.subscribedRecords.remove(recordNameToSubscribe))
            {
                this.context.removeObserver(this.model, recordNameToSubscribe);
            }
        }
    }

    static class RpcView extends AbstractPlatformDesktopView
    {
        private final static int indexRpcName = indexCustomStart;
        private final static int indexRpcDefinition = indexCustomStart + 1;

        String rpcName;
        String rpcDefinition;

        RpcView(PlatformDesktop desktop, PlatformMetaDataViewEnum metaDataViewType, String metaDataViewKey)
        {
            super("RPC" + _SEP_ + metaDataViewKey, desktop, metaDataViewType, metaDataViewKey);
        }

        private void initialise(String rpcName, String rpcDefinition)
        {
            this.rpcName = rpcName;
            this.rpcDefinition = rpcDefinition;

            this.frame.setTitle(this.frame.getTitle() + "." + rpcName);

            final RpcInstance instance = RpcInstance.constructInstanceFromDefinition(rpcName, rpcDefinition);

            final JTextField result = new JTextField("No result");
            result.setEditable(false);
            final ParametersPanel parameters = new ParametersPanel();
            parameters.setOkButtonActionListener(e -> {
                result.setText("Executing...");
                parameters.setEnabled(false);

                getExecutor().execute(() -> {
                    try
                    {
                        // get the args in correct order
                        final String[] argNames = getArgNames(instance);
                        final TypeEnum[] argTypes = instance.getArgTypes();
                        final IValue[] rpcArgs = new IValue[argTypes.length];
                        final Map<String, String> map = parameters.get();
                        for (int i = 0; i < argTypes.length; i++)
                        {
                            rpcArgs[i] = argTypes[i].fromString(map.get(getParamName(argTypes, argNames, i)));
                        }
                        try
                        {
                            final IValue executeRpcResult =
                                    RpcView.this.desktop.getMetaDataModel().executeRpc(this.context,
                                            instance.getName(), rpcArgs);
                            final String textValue;
                            if (executeRpcResult != null)
                            {
                                textValue = executeRpcResult.textValue();
                            }
                            else
                            {
                                textValue = null;
                            }
                            SwingUtilities.invokeLater(() -> result.setText(textValue));
                        }
                        catch (final Exception e1)
                        {
                            SwingUtilities.invokeLater(() -> {
                                String message = e1.getMessage();
                                if (message == null || message.length() == 0)
                                {
                                    message = e1.getClass().getSimpleName();
                                }
                                result.setText(message);
                            });
                        }
                    }
                    finally
                    {
                        SwingUtilities.invokeLater(() -> parameters.setEnabled(true));
                    }
                });
            });

            final TypeEnum[] argTypes = instance.getArgTypes();
            final String[] argNames = getArgNames(instance);
            for (int i = 0; i < argTypes.length; i++)
            {
                parameters.addParameter(getParamName(argTypes, argNames, i), "");
            }
            JPanel panel = new JPanel(new BorderLayout());
            panel.add(parameters);
            panel.add(result, BorderLayout.SOUTH);
            this.frame.add(panel);
        }

        static String[] getArgNames(final IRpcInstance instance)
        {
            String[] argNames = instance.getArgNames();
            if (argNames == null)
            {
                argNames = new String[instance.getArgTypes().length];
                for (int i = 0; i < argNames.length; i++)
                {
                    argNames[i] = "Arg" + i;
                }
            }
            return argNames;
        }

        static String getParamName(final TypeEnum[] argTypes, final Object[] argNames, int i)
        {
            return argNames[i] + " (" + argTypes[i].name() + ")";
        }

        @Override
        void toResolvableState(StringBuilder sb)
        {
            sb.append(this.rpcName).append(","); // 8
            sb.append(this.rpcDefinition.replace(",", "-comma-")).append(","); //9
        }

        @Override
        void resolveFromStateString(String[] tokens)
        {
            String rpcName = tokens[indexRpcName];
            String rpcDefinition = tokens[indexRpcDefinition].replace("-comma-", ",");
            initialise(rpcName, rpcDefinition);
        }

        @Override
        protected void destroy()
        {
        }
    }

    static class MetaDataView extends AbstractPlatformDesktopView
    {
        final IRecordListener statusObserver;
        final RowOrientedRecordTable table;
        final RowOrientedRecordTableModel model;
        final TableSummaryPanel tableSummary;

        MetaDataView(PlatformDesktop platformDesktop, PlatformMetaDataViewEnum metaDataViewType,
                String metaDataViewKey)
        {
            super(metaDataViewType + ((metaDataViewKey == null || metaDataViewKey.isEmpty()) ? "" :
                    _SEP_ + metaDataViewKey), platformDesktop, metaDataViewType, metaDataViewKey);

            this.model = new RowOrientedRecordTableModel();
            // todo need a filter on the table
            this.table = new RowOrientedRecordTable(this.model);
            this.statusObserver = new TableStatusListener(this.table);

            this.context.addObserver(this.sessionId, this.statusObserver, ISystemRecordNames.CONTEXT_STATUS);

            this.metaDataViewType.register(this.context, this.model, this.metaDataViewKey);
            this.model.addRecordRemovedListener(this.context);

            prepareTablePopupMenu();

            this.frame.add(new JScrollPane(this.table));
            this.tableSummary = new TableSummaryPanel(this.table.getModel());
            this.frame.add(this.tableSummary, BorderLayout.SOUTH);

            try
            {
                this.frame.setSelected(true);
            }
            catch (PropertyVetoException e)
            {
                Log.log(this, "Could not set frame to front for " + this.title, e);
            }
        }

        private void prepareTablePopupMenu()
        {
            JPopupMenu popupMenu = new JPopupMenu();
            for (final PlatformMetaDataViewEnum type : this.metaDataViewType.getChildViews())
            {
                JMenuItem menuItem = new JMenuItem();
                final Action action = type.createActionToOpenChildView(this.desktop, this);
                menuItem.setAction(action);
                menuItem.setText(action.getValue(Action.NAME).toString());
                popupMenu.add(menuItem);
            }
            this.table.setComponentPopupMenu(popupMenu);
        }

        @Override
        void toResolvableState(StringBuilder sb)
        {
            sb.append(this.table.toStateString()); // the rest
        }

        @Override
        void resolveFromStateString(String[] tokens)
        {
            this.table.fromStateString(getTableStateString(tokens, indexCustomStart));
        }

        @Override
        protected void destroy()
        {
            this.tableSummary.destroy();
            this.metaDataViewType.deregister(this.model);
            this.table.setComponentPopupMenu(null);

            this.context.executeSequentialCoreTask(new ISequentialRunnable()
            {
                @Override
                public Object context()
                {
                    return MetaDataView.this;
                }

                @Override
                public void run()
                {
                    MetaDataView.this.context.removeObserver(MetaDataView.this.statusObserver,
                            ISystemRecordNames.CONTEXT_STATUS);
                    MetaDataView.this.model.removeRecordRemovedListener(MetaDataView.this.context);
                    PlatformMetaDataViewEnum.deregister(MetaDataView.this.context, MetaDataView.this.model);
                }
            });
        }

        IRecord getSelectedRecord()
        {
            return this.table.getSelectedRecord();
        }
    }

    /**
     * Displays summary information for the desktop environment
     *
     * @author Ramon Servadei
     */
    static final class DesktopSummaryPanel extends JPanel
    {
        private static final double inverse_1MB = 1d / (1024 * 1024);
        private static final long serialVersionUID = 1L;

        final Thread t;

        final SummaryField dataCount;
        final SummaryField msgCount;
        final JProgressBar memory;
        final JProgressBar msgsPerSec;

        DesktopSummaryPanel(IObserverContext registryProxy)
        {
            this.dataCount = new SummaryField("Kb Rx", false);
            this.msgCount = new SummaryField("Msgs Rx", false);

            this.memory = new JProgressBar();
            this.memory.setStringPainted(true);
            this.memory.setMaximum(100);

            this.msgsPerSec = new JProgressBar();
            this.msgsPerSec.setMaximum(50);
            this.msgsPerSec.setStringPainted(true);

            final String name = registryProxy.getName();
            // todo replace with he new idea of the agent-level inbound throughput listener
            registryProxy.addObserver((image, atomicChange) -> {
                for (String connection : atomicChange.getSubMapKeys())
                {
                    final IValue proxyId =
                            image.getOrCreateSubMap(connection).get(IContextConnectionsRecordFields.PROXY_ID);
                    if (proxyId != null && proxyId.textValue().contains(name))
                    {
                        final Map<String, IValue> putEntries =
                                atomicChange.getSubMapAtomicChange(connection).getPutEntries();
                        if (putEntries.size() > 0)
                        {
                            final IValue msgsPerSec = image.getOrCreateSubMap(connection).get(
                                    IContextConnectionsRecordFields.MSGS_PER_SEC);
                            final IValue kbsPerSec = image.getOrCreateSubMap(connection).get(
                                    IContextConnectionsRecordFields.KB_PER_SEC);
                            if (msgsPerSec != null)
                            {
                                SwingUtilities.invokeLater(() -> {
                                    DesktopSummaryPanel.this.msgsPerSec.setValue(
                                            (int) msgsPerSec.longValue());
                                    DesktopSummaryPanel.this.msgsPerSec.setString(
                                            msgsPerSec.textValue() + " (" + kbsPerSec.textValue() + " kb/s)");
                                });
                            }
                            final IValue msgCountVal = image.getOrCreateSubMap(connection).get(
                                    IContextConnectionsRecordFields.MESSAGE_COUNT);
                            if (msgCountVal != null)
                            {
                                SwingUtilities.invokeLater(
                                        () -> DesktopSummaryPanel.this.msgCount.setValue(msgCountVal));
                            }
                            final IValue kbCountVal = image.getOrCreateSubMap(connection).get(
                                    IContextConnectionsRecordFields.KB_COUNT);
                            if (kbCountVal != null)
                            {
                                SwingUtilities.invokeLater(
                                        () -> DesktopSummaryPanel.this.dataCount.setValue(kbCountVal));
                            }
                        }
                    }
                }
            }, ProxyContext.IRemoteSystemRecordNames.REMOTE_CONTEXT_CONNECTIONS);

            final FlowLayout layout = new FlowLayout(FlowLayout.RIGHT);
            layout.setHgap(0);
            setLayout(layout);

            this.dataCount.addTo(this);
            this.dataCount.setValue(TextValue.valueOf(""));
            this.msgCount.addTo(this);
            this.msgCount.setValue(TextValue.valueOf(""));

            add(new JLabel("Msgs/s"));
            add(this.msgsPerSec);
            add(Box.createHorizontalStrut(6));
            add(new JLabel("Mem"));
            add(this.memory);
            this.t = ThreadUtils.newThread(() -> {
                while (true)
                {
                    final long used =
                            (long) ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
                                    * inverse_1MB);
                    final long max = (long) (Runtime.getRuntime().totalMemory() * inverse_1MB);
                    final String text = used + " / " + max + "M";
                    SwingUtilities.invokeLater(() -> {
                        DesktopSummaryPanel.this.memory.setValue((int) (((double) used / max) * 100));
                        DesktopSummaryPanel.this.memory.setString(text);
                    });
                    ThreadUtils.sleep(1000);
                }
            }, "desktop-summary-panel");
            this.t.start();
        }
    }

    static final CompoundBorder LINE_BORDER =
            BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.black),
                    BorderFactory.createEmptyBorder(0, 6, 1, 6));
    static final CompoundBorder ETCHED_BORDER =
            BorderFactory.createCompoundBorder(BorderFactory.createEtchedBorder(),
                    BorderFactory.createEmptyBorder(0, 6, 1, 6));

    static final class SummaryField
    {
        final JLabel component;
        final String fieldName;

        SummaryField(String fieldName)
        {
            this(fieldName, true);
        }

        SummaryField(String fieldName, boolean lineBorder)
        {
            this.component = new JLabel();
            this.component.setBorder(lineBorder ? LINE_BORDER : ETCHED_BORDER);

            this.fieldName = fieldName;
        }

        void addTo(JPanel panel)
        {
            panel.add(this.component);
            panel.add(Box.createHorizontalStrut(6));
        }

        void setValue(IValue value)
        {
            if (value != null)
            {
                this.component.setText(this.fieldName + ": " + value.textValue());
            }
        }
    }

    /**
     * Displays summary information for the ClearConnect platform
     *
     * @author Ramon Servadei
     */
    static final class PlatformSummaryPanel extends JPanel
    {
        private static final long serialVersionUID = 1L;

        final SummaryField version;
        final SummaryField uptime;
        final SummaryField nodeCount;
        final SummaryField agentCount;
        final SummaryField serviceCount;
        final SummaryField serviceInstanceCount;

        PlatformSummaryPanel(IObserverContext registryProxy)
        {
            this.version = new SummaryField("Registry");
            this.uptime = new SummaryField("Uptime");
            this.nodeCount = new SummaryField("Nodes");
            this.agentCount = new SummaryField("Agents");
            this.serviceCount = new SummaryField("Services");
            this.serviceInstanceCount = new SummaryField("Instances");

            registryProxy.addObserver((image, atomicChange) -> {
                final Map<String, IValue> copy = new HashMap<>(image.asFlattenedMap());
                SwingUtilities.invokeLater(() -> {
                    PlatformSummaryPanel.this.nodeCount.setValue(
                            copy.get(IPlatformSummaryRecordFields.NODES));
                    PlatformSummaryPanel.this.agentCount.setValue(
                            copy.get(IPlatformSummaryRecordFields.AGENTS));
                    PlatformSummaryPanel.this.serviceCount.setValue(
                            copy.get(IPlatformSummaryRecordFields.SERVICES));
                    PlatformSummaryPanel.this.serviceInstanceCount.setValue(
                            copy.get(IPlatformSummaryRecordFields.SERVICE_INSTANCES));
                    PlatformSummaryPanel.this.version.setValue(
                            copy.get(IPlatformSummaryRecordFields.VERSION));
                    PlatformSummaryPanel.this.uptime.setValue(copy.get(IPlatformSummaryRecordFields.UPTIME));
                });
            }, IRegistryRecordNames.PLATFORM_SUMMARY);

            final FlowLayout layout = new FlowLayout(FlowLayout.CENTER);
            layout.setHgap(0);
            setLayout(layout);
            this.version.addTo(this);
            this.uptime.addTo(this);
            this.nodeCount.addTo(this);
            this.agentCount.addTo(this);
            this.serviceCount.addTo(this);
            this.serviceInstanceCount.addTo(this);

            setBackground(Color.WHITE);
        }
    }

    /**
     * Mini summary of rows,cols for a {@link TableModel}
     *
     * @author Ramon Servadei
     */
    static final class TableSummaryPanel extends JPanel
    {
        private static final long serialVersionUID = 1L;
        final JLabel rows, columns;
        final TableModel model;
        final TableModelListener tableModelListener;

        TableSummaryPanel(TableModel model)
        {
            setBorder(BorderFactory.createEmptyBorder());
            this.model = model;
            this.rows = new JLabel();
            this.rows.setBorder(BorderFactory.createEmptyBorder());
            this.columns = new JLabel();
            this.columns.setBorder(BorderFactory.createEmptyBorder());
            final FlowLayout layout = new FlowLayout(FlowLayout.RIGHT);
            layout.setVgap(0);
            setLayout(layout);
            add(this.rows);
            add(this.columns);

            this.tableModelListener = e -> {
                String text = "rows:" + TableSummaryPanel.this.model.getRowCount();
                if (!text.equals(TableSummaryPanel.this.rows.getText()))
                {
                    TableSummaryPanel.this.rows.setText(text);
                }

                text = "cols:" + TableSummaryPanel.this.model.getColumnCount();
                if (!text.equals(TableSummaryPanel.this.columns.getText()))
                {
                    TableSummaryPanel.this.columns.setText(text);
                }
            };
            model.addTableModelListener(this.tableModelListener);
        }

        void destroy()
        {
            removeAll();
            this.model.removeTableModelListener(this.tableModelListener);
        }
    }

    /**
     * Base class for a GUI that gathers parameters
     *
     * @author Ramon Servadei
     */
    static class ParametersPanel extends JPanel
    {
        /**
         * Encapsulates a {@link JLabel} and {@link JTextField} for a single parameter
         *
         * @author Ramon Servadei
         */
        private static final class Parameter
        {
            final JLabel name;

            final JTextField value;

            Parameter(JPanel gridLayoutPanel, String name, String value)
            {
                super();
                this.name = new JLabel(name);
                this.name.setBorder(BorderFactory.createEtchedBorder());
                this.value = new JTextField(value);
                gridLayoutPanel.add(this.name);
                gridLayoutPanel.add(this.value);
            }

            String getValue()
            {
                return this.value.getText();
            }
        }

        private static final long serialVersionUID = 1L;

        final JButton ok;
        final LinkedHashMap<String, Parameter> parameters;
        final JPanel parametersPanel;
        final AtomicReference<LinkedHashMap<String, String>> result;
        ActionListener actionListener;

        ParametersPanel()
        {
            this.ok = new JButton("OK");
            this.parameters = new LinkedHashMap<>();
            this.parametersPanel = new JPanel(new GridLayout(0, 2));
            this.result = new AtomicReference<>();

            this.ok.addActionListener(e -> {
                LinkedHashMap<String, String> values = new LinkedHashMap<>();
                Map.Entry<String, Parameter> entry;
                String key;
                Parameter value;
                for (Map.Entry<String, Parameter> stringParameterEntry : ParametersPanel.this.parameters.entrySet())
                {
                    entry = stringParameterEntry;
                    key = entry.getKey();
                    value = entry.getValue();
                    values.put(key, value.getValue());
                }
                ParametersPanel.this.result.set(values);
                synchronized (ParametersPanel.this.result)
                {
                    ParametersPanel.this.result.notify();
                }

                if (ParametersPanel.this.actionListener != null)
                {
                    ParametersPanel.this.actionListener.actionPerformed(e);
                }
            });

            setLayout(new BorderLayout());
            add(new JScrollPane(this.parametersPanel));
            add(this.ok, BorderLayout.SOUTH);
        }

        @Override
        public void setEnabled(boolean enabled)
        {
            super.setEnabled(enabled);
            this.ok.setEnabled(enabled);
            final Collection<Parameter> values = this.parameters.values();
            for (Parameter parameter : values)
            {
                parameter.value.setEnabled(enabled);
            }
        }

        void addParameter(String name, String defaultValue)
        {
            final Parameter parameter = new Parameter(this.parametersPanel, name, defaultValue);
            this.parameters.put(name, parameter);
        }

        void setOkButtonActionListener(ActionListener actionListener)
        {
            this.actionListener = actionListener;
        }

        LinkedHashMap<String, String> get()
        {
            synchronized (this.result)
            {
                LinkedHashMap<String, String> params;
                while ((params = this.result.getAndSet(null)) == null)
                {
                    try
                    {
                        this.result.wait();
                    }
                    catch (InterruptedException e)
                    {
                        // don't care
                    }
                }
                return params;
            }
        }
    }

    /**
     * This enum expresses the types of views of the meta data that exist in the model. A meta data
     * view type can declare if it has child views which can be opened using one of the elements of
     * the current view as the key for the child view.
     *
     * @author Ramon Servadei
     */
    enum PlatformMetaDataViewEnum
    {
        // data views
        RPC(RpcView.class),

        INSTANCE_RPC(RpcView.class),

        RECORD(RecordSubscriptionView.class),

        INSTANCE_RECORD(RecordSubscriptionView.class),

        // grouping views
        RPCS_PER_SERVICE(MetaDataView.class, RPC),

        RECORDS_PER_SERVICE(MetaDataView.class, RECORD),

        RPCS_PER_INSTANCE(MetaDataView.class, INSTANCE_RPC),

        RECORDS_PER_INSTANCE(MetaDataView.class, INSTANCE_RECORD),

        CLIENTS_PER_SERVICE(MetaDataView.class),

        CLIENTS_PER_INSTANCE(MetaDataView.class),

        INSTANCES_PER_SERVICE(MetaDataView.class, "Service", CLIENTS_PER_INSTANCE, RECORDS_PER_INSTANCE,
                RPCS_PER_INSTANCE),

        // the main views
        AGENTS(MetaDataView.class),

        SERVICES(MetaDataView.class, INSTANCES_PER_SERVICE, CLIENTS_PER_SERVICE, RECORDS_PER_SERVICE,
                RPCS_PER_SERVICE),

        NODES(MetaDataView.class),

        ;
        static final Map<String, RecordSubscriptionView> recordSubscriptionViews = new HashMap<>();

        static void deregister(IObserverContext context, IRecordListener observer)
        {
            final Set<String> recordNames = context.getRecordNames();
            for (String recordName : recordNames)
            {
                context.removeObserver(observer, recordName);
            }
        }

        final PlatformMetaDataViewEnum[] childViews;
        /**
         * The key to link with the parent's child view key
         */
        final String filterViewKey;
        /**
         * The key to link with child view's parent key
         */
        final Class<? extends AbstractPlatformDesktopView> viewClass;
        /**
         * Ref to the registration manager managing the "all records listener"
         */
        final Map<IRecordListener, ContextUtils.AllRecordsRegistrationManager> mappedAllRecordsListeners =
                new HashMap<>();

        PlatformMetaDataViewEnum(Class<? extends AbstractPlatformDesktopView> viewClass, String filterViewKey,
                PlatformMetaDataViewEnum... childViews)
        {
            this.viewClass = viewClass;
            this.filterViewKey = filterViewKey;
            this.childViews = childViews;
        }

        PlatformMetaDataViewEnum(Class<? extends AbstractPlatformDesktopView> viewClass,
                PlatformMetaDataViewEnum... childViews)
        {
            this(viewClass, null, childViews);
        }

        Action createActionToOpenChildView(final PlatformDesktop desktop,
                final AbstractPlatformDesktopView parentView)
        {
            return new AbstractAction("Show " + PlatformMetaDataViewEnum.this)
            {
                private static final long serialVersionUID = 1L;

                @SuppressWarnings("unused")
                @Override
                public void actionPerformed(ActionEvent e)
                {
                    final String viewType = PlatformMetaDataViewEnum.this.toString();
                    boolean create = true;

                    // check if we already have created this window
                    String viewKey = null;

                    if (parentView != null)
                    {
                        final IRecord selectedRecord = parentView.getSelectedRecord();
                        viewKey = selectedRecord.getName();
                    }

                    final String lookingFor = viewKey == null ? viewType : viewType + _SEP_ + viewKey;
                    for (AbstractPlatformDesktopView view : desktop.views)
                    {
                        if (is.eq(lookingFor, view.title) && is.eq(viewKey, view.metaDataViewKey) && is.eq(
                                PlatformMetaDataViewEnum.this, view.metaDataViewType))
                        {
                            view.frame.toFront();
                            final Border previousBorder = view.frame.getBorder();
                            view.frame.setBorder(BorderFactory.createLineBorder(Color.RED, 4));
                            final AbstractPlatformDesktopView duplicate = view;
                            ThreadUtils.newThread(() -> {
                                ThreadUtils.sleep(500);
                                SwingUtilities.invokeLater(() -> duplicate.frame.setBorder(previousBorder));
                            }, MetaDataView.class.getSimpleName() + "-highlighter").start();
                            create = false;
                            break;
                        }
                    }

                    if (create)
                    {
                        if (PlatformMetaDataViewEnum.this.viewClass == MetaDataView.class)
                        {
                            new MetaDataView(desktop, PlatformMetaDataViewEnum.this, viewKey);
                        }
                        else if (PlatformMetaDataViewEnum.this.viewClass == RecordSubscriptionView.class)
                        {
                            synchronized (recordSubscriptionViews)
                            {
                                final String nameOfServiceOrServiceInstance = parentView.metaDataViewKey;
                                RecordSubscriptionView view =
                                        recordSubscriptionViews.get(nameOfServiceOrServiceInstance);
                                if (view == null)
                                {
                                    // NOTE: the constructor registers with the
                                    // recordSubscriptionViews
                                    view = new RecordSubscriptionView(desktop, PlatformMetaDataViewEnum.this,
                                            nameOfServiceOrServiceInstance);
                                }
                                else
                                {
                                    view.frame.toFront();
                                    final Border previousBorder = view.frame.getBorder();
                                    view.frame.setBorder(BorderFactory.createLineBorder(Color.RED, 4));
                                    final RecordSubscriptionView duplicate = view;
                                    ThreadUtils.newThread(() -> {
                                        ThreadUtils.sleep(500);
                                        SwingUtilities.invokeLater(
                                                () -> duplicate.frame.setBorder(previousBorder));
                                    }, MetaDataView.class.getSimpleName() + "-highlighter").start();
                                }
                                view.subscribeFor(parentView.getSelectedRecord().getName());
                            }
                        }
                        else if (PlatformMetaDataViewEnum.this.viewClass == RpcView.class)
                        {
                            final String nameOfServiceOrServiceInstance = parentView.metaDataViewKey;
                            final String rpcName = parentView.getSelectedRecord().getName();
                            final String rpcDefinition =
                                    parentView.getSelectedRecord().get("args").textValue();

                            new RpcView(desktop, PlatformMetaDataViewEnum.this,
                                    nameOfServiceOrServiceInstance).initialise(rpcName, rpcDefinition);
                        }
                    }
                }
            };
        }

        PlatformMetaDataViewEnum[] getChildViews()
        {
            return this.childViews;
        }

        void register(final IObserverContext context, final IRecordListener observer,
                final String parentViewKey)
        {
            // listen for all records (added and removed)
            final IRecordListener listener = (imageCopy, atomicChange) -> {
                if (PlatformMetaDataViewEnum.this.filterViewKey == null || parentViewKey == null)
                {
                    observer.onChange(imageCopy, atomicChange);
                    return;
                }

                IValue recordParentViewKey = imageCopy.get(PlatformMetaDataViewEnum.this.filterViewKey);
                if (recordParentViewKey != null && parentViewKey.equals(recordParentViewKey.textValue()))
                {
                    observer.onChange(imageCopy, atomicChange);
                }
            };
            this.mappedAllRecordsListeners.put(observer,
                    ContextUtils.addAllRecordsListener(context, listener));
        }

        void deregister(final IRecordListener observer)
        {
            final ContextUtils.AllRecordsRegistrationManager allRecordsRegistrationManager =
                    this.mappedAllRecordsListeners.remove(observer);
            if (allRecordsRegistrationManager != null)
            {
                allRecordsRegistrationManager.destroy();
            }
        }

        IObserverContext getContextForMetaDataViewType(PlatformMetaDataModel model, String contextKey)
        {
            switch(this)
            {
                case AGENTS:
                    return model.getPlatformRegistryAgentsContext();
                case CLIENTS_PER_INSTANCE:
                    return model.getPlatformServiceInstanceConnectionsContext(contextKey);
                case CLIENTS_PER_SERVICE:
                    return model.getPlatformServiceConnectionsContext(contextKey);
                case NODES:
                    return model.getPlatformNodesContext();
                case SERVICES:
                    return model.getPlatformServicesContext();
                case INSTANCES_PER_SERVICE:
                    return model.getPlatformServiceInstancesContext();
                case RECORDS_PER_INSTANCE:
                    return model.getPlatformServiceInstanceRecordsContext(contextKey);
                case RECORDS_PER_SERVICE:
                    return model.getPlatformServiceRecordsContext(contextKey);
                case RPCS_PER_INSTANCE:
                    return model.getPlatformServiceInstanceRpcsContext(contextKey);
                case RPCS_PER_SERVICE:
                    return model.getPlatformServiceRpcsContext(contextKey);
                case INSTANCE_RECORD:
                case INSTANCE_RPC:
                    return model.getProxyContextForPlatformServiceInstance(contextKey);
                case RECORD:
                case RPC:
                    return model.getProxyContextForPlatformService(contextKey);
                default:
                    throw new IllegalStateException("no support for " + this);
            }
        }

    }

    static String toStateString(PlatformDesktop platformDesktop)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(platformDesktop.getDesktopWindow().getX()).append(","); // 0
        sb.append(platformDesktop.getDesktopWindow().getY()).append(","); // 1
        sb.append(platformDesktop.getDesktopWindow().getWidth()).append(","); // 2
        sb.append(platformDesktop.getDesktopWindow().getHeight()).append(","); // 3
        return sb.toString();
    }

    static void fromStateString(PlatformDesktop platformDesktop, String stateString)
    {
        final String[] tokens = stateString.split(",");
        try
        {
            platformDesktop.getDesktopWindow().setLocation(
                    new Point(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1])));
            platformDesktop.getDesktopWindow().setSize(Integer.parseInt(tokens[2]),
                    Integer.parseInt(tokens[3]));
        }
        catch (Exception e)
        {
            Log.log(PlatformDesktop.class, "Could not set position or size for '" + stateString + "'", e);
        }
    }

    private final PlatformMetaDataModel platformMetaDataModel;
    final Set<AbstractPlatformDesktopView> views;
    JDesktopPane desktopPane;
    JFrame desktopWindow;

    PlatformDesktop(final PlatformMetaDataModel platformMetaDataModel)
    {
        try
        {
            UIManager.setLookAndFeel("com.sun.java.swing.plaf.nimbus.NimbusLookAndFeel");
            UIManager.put("TableHeaderUI", FimtraTableHeaderUI.class.getName());
        }
        catch (Exception e)
        {
            Log.log(PlatformDesktop.class, "Could not set NimbusLookAndFeel", e);
            try
            {
                UIManager.setLookAndFeel("com.sun.java.swing.plaf.nimbus.WindowsLookAndFeel");
            }
            catch (Exception e2)
            {
                Log.log(PlatformDesktop.class, "Could not set WindowsLookAndFeel", e2);
            }
        }
        this.platformMetaDataModel = platformMetaDataModel;
        this.views = new HashSet<>();
        SwingUtilities.invokeLater(new Runnable()
        {
            @Override
            public void run()
            {
                PlatformDesktop.this.desktopPane = new JDesktopPane();

                PlatformDesktop.this.desktopWindow = new JFrame();
                PlatformDesktop.this.desktopWindow.setIconImage(createIcon());
                PlatformDesktop.this.desktopWindow.add(
                        new PlatformSummaryPanel(platformMetaDataModel.agent.registryProxy),
                        BorderLayout.NORTH);
                PlatformDesktop.this.desktopWindow.add(
                        new DesktopSummaryPanel(platformMetaDataModel.agent.registryProxy),
                        BorderLayout.SOUTH);

                platformMetaDataModel.agent.addRegistryAvailableListener(
                        EventListenerUtils.synchronizedListener(new IRegistryAvailableListener()
                        {
                            String platformName;

                            @Override
                            public void onRegistryDisconnected()
                            {
                                setTitle(this.platformName + " [DISCONNECTED]");
                                PlatformDesktop.this.desktopPane.setEnabled(false);
                            }

                            @Override
                            public void onRegistryConnected()
                            {
                                this.platformName =
                                        PlatformDesktop.this.getMetaDataModel().agent.getPlatformName();
                                setTitle(this.platformName + " [CONNECTED]");
                                PlatformDesktop.this.desktopPane.setEnabled(true);
                            }

                            private void setTitle(String title)
                            {
                                PlatformDesktop.this.desktopWindow.setTitle("ClearConnect | " + title + " "
                                        + PlatformDesktop.this.getMetaDataModel().agent.registryProxy.getShortSocketDescription());
                            }
                        }));

                PlatformDesktop.this.getDesktopWindow().addWindowListener(new WindowAdapter()
                {
                    @Override
                    public void windowClosing(WindowEvent e)
                    {
                        super.windowClosing(e);
                        saveDesktopPlatformViewState();
                    }
                });
                PlatformDesktop.this.getDesktopWindow().setDefaultCloseOperation(
                        WindowConstants.EXIT_ON_CLOSE);
                PlatformDesktop.this.getDesktopWindow().getContentPane().add(
                        PlatformDesktop.this.getDesktopPane(), BorderLayout.CENTER);
                PlatformDesktop.this.getDesktopWindow().setSize(640, 480);

                JPopupMenu popupMenu = new JPopupMenu();
                for (final PlatformMetaDataViewEnum type : new PlatformMetaDataViewEnum[] {
                        // todo need to re-add support for NODES
                        // PlatformMetaDataViewEnum.NODES,
                        PlatformMetaDataViewEnum.SERVICES, PlatformMetaDataViewEnum.AGENTS, })
                {
                    JMenuItem menuItem = new JMenuItem();
                    menuItem.setAction(type.createActionToOpenChildView(PlatformDesktop.this, null));
                    popupMenu.add(menuItem);
                }
                PlatformDesktop.this.getDesktopPane().setComponentPopupMenu(popupMenu);

                loadDesktopPlatformViewState();

                PlatformDesktop.this.getDesktopWindow().setVisible(true);
            }
        });
    }

    JDesktopPane getDesktopPane()
    {
        return this.desktopPane;
    }

    PlatformMetaDataModel getMetaDataModel()
    {
        return this.platformMetaDataModel;
    }

    Set<AbstractPlatformDesktopView> getViews()
    {
        return this.views;
    }

    String getStateFileName()
    {
        return "." + File.separator + PlatformMetaDataModel.class.getSimpleName() + "_"
                + this.getMetaDataModel().getAgent().getPlatformName() + ".ini";
    }

    JFrame getDesktopWindow()
    {
        return this.desktopWindow;
    }

    void saveDesktopPlatformViewState()
    {
        try
        {
            File stateFile = new File(getStateFileName());
            if (stateFile.exists() || stateFile.createNewFile())
            {
                PrintWriter pw = new PrintWriter(stateFile);
                pw.println(toStateString(this));
                for (AbstractPlatformDesktopView view : getViews())
                {
                    pw.println(view.toStateString());
                }
                pw.flush();
                pw.close();
            }
            else
            {
                throw new IOException("Could not create file " + stateFile);
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not save state", e);
        }
    }

    void loadDesktopPlatformViewState()
    {
        BufferedReader br = null;
        try
        {
            File stateFile = new File(getStateFileName());
            if (stateFile.exists())
            {
                br = new BufferedReader(new FileReader(stateFile));
                if (br.ready())
                {
                    fromStateString(this, br.readLine());
                    String line;
                    while (br.ready())
                    {
                        line = br.readLine();
                        try
                        {
                            AbstractPlatformDesktopView.fromStateString(this, line);
                        }
                        catch (Exception e)
                        {
                            Log.log(PlatformDesktop.this, "Could not view from state: " + line, e);
                        }
                    }
                }
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not read state", e);
        }
        finally
        {
            FileUtils.safeClose(br);
        }
    }

    static Image createIcon()
    {
        BlobValue hexIcon = new BlobValue(
                "89504E470D0A1A0A0000000D4948445200000020000000200806000000737A7AF400000006624B474400FF00FF00FFA0BDA793000000097048597300000B1300000B1301009A9C180000000774494D4507D"
                        + "F061D140E0C8E9514B10000001974455874436F6D6D656E74004372656174656420776974682047494D5057810E17000004464944415458C3ED576D4C5B65147EEEBDED6D3BCA5A4A296360658389B5930F2D68DC1"
                        + "7CA20B0397F8C4432974516232386A4C60F66A624334A66430C81B14D162251C130A2811FD3CCC9624686B0216ECE31661719DD1C2B6C6BF9680BEDED7DAF3F26464A5B6881F867E7DF3DEF79CF79EE39CF39EFFB0"
                        + "20F254C51676D57669AAEFCF2F4C78396559BCB92C3F54385BBD150D9F7131391910D005E97DDFB5BE53AA9D769E343F5C384133CB5BC658B449D7B70E69B16CB680AE4F6B8F94C5FA8BEE87000C8566DFC5610845"
                        + "9BAD52FBC57AF36EC512C3B80F40F7E2C031D1FEDAB1728203AE3E5A3CB5D82156B8A8E9F1388FF6DD2D8942769B1AC65C2DC717F593260A8BC5C473869600341802A6DE7A9252F41DCF34588CF2DD08956EAF7024"
                        + "2707EC4AE4B8CDDB02F71C9DB30ABEA662F254E30CCE7CE71E7AAA5DFA44F5CD20CE8DEA829A4A58F040F4E5118F9A3ABB75C77FE5A5373737F6969E9530BF12D5A88912265EF31810F9C7A8AA63076F5878EBA6DD"
                        + "3295EA2CA040083C1D00E40BBE82EC83878CA24923D9117CCC6F6EB89DA8A2DE372312BCD9CD14546462A9292921C9D9D9DDD8B2901CB2AB38DBE43E75FD2F31C86DADE7CFDFA17BB8C77ACA3F6FFAEF13C0F9D4E7"
                        + "70800AB52A9420320962B1F10EFD3FED38044E2CF86789C9E4B1F253F3372B6B60100BABBBB5B286A36A7E572B9B8BABABAD166B3850680738C8166104F31299BFDB59DD77977A2B73C32C66DBF796146D7D6D6769"
                        + "6E7671345100468B5DA5700248404808D5251CF1EB65F0268CAB7CDA64606FEEC7B3F560960C2E78F5D8410D71C92310C1A1B1B4F860440AE7D0EBC5BAEF065FAC4F58EEF2E1FD22723C034621866D29F3E2A2A2A2"
                        + "D2F2F6FC7BC0018B90C001053B6FEB57BE4DD7AFBC5933F5314054A44E1DE85A6CA8123B92FEEBC12B81DAD56EBEF81D68A8B8B3F9FB70D058F178F35BC531E919558CBAE89CE1265D9341E4597DD6E3E513758B3F"
                        + "F00000C1CFD302000BD5EAFD06AB5DBFC125B2C5E11171727EDE9E939E377140B8200EDFEDDCA9857B3EDC4C3CD1934C4EDF51237D7E3E8351FBBF156FDD7013024B4B7B7DF0A0490E7796F6161A104009953028AA"
                        + "2B07283EE886F70001088004ACC8818B974A3726B7A73DAF91ACFFA8E4FDAD656EFDBEE63FA17C771D301A71EC388AAAAAA4EFBCDC0A3157B52A2776DBA46DC5C0867290DE29C9AF2DE9F6C9A32DFFEF2C6DBC7CFB"
                        + "5B6B6DE62593621D836A3D19868B1582CB300A476565FA423D9F4B0AFC9340DDE36396A1C525BD551AAD460A6A3A3A3C325252509111111020D008F7F756007A394841F1C000801A3926B7AAD8363BE13D157341AC"
                        + "DEAA2A2A2AD4EA7F30107D8B5EA8660A7DDC24108E81A360F33F4FCA77C4141C1F700281A00BC132EC792BC56286048E2EE9F9E9E9F482E976BF29FBB2CC0C647C7682B767FC3AC946916139FBB3B6E19347EF6524"
                        + "E4ECEA6FCFCFCC3344D8B04412084109E104200F00008C7712E93C954E07038261FBE31FF77F91B46228F9FE19DE6B40000000049454E44AE426082");

        return new ImageIcon(hexIcon.byteValue()).getImage();
    }

    /**
     * @param args - Optional. args[0]=node IP, args[1]=registry port (optional)
     */
    public static void main(String[] args) throws Exception
    {
        try
        {
            UIManager.setLookAndFeel("com.sun.java.swing.plaf.nimbus.NimbusLookAndFeel");
        }
        catch (Exception e)
        {
            Log.log(PlatformDesktop.class, "Could not set NimbusLookAndFeel", e);
            try
            {
                UIManager.setLookAndFeel("com.sun.java.swing.plaf.nimbus.WindowsLookAndFeel");
            }
            catch (Exception e2)
            {
                Log.log(PlatformDesktop.class, "Could not set WindowsLookAndFeel", e2);
            }
        }

        Context.log = false;

        final String node = "Platform node";
        final String port = "platform port";

        final String nodeArg = args.length > 0 ? args[0] : TcpChannelUtils.LOCALHOST_IP;
        final int registryPortArg = args.length > 1 ? Integer.valueOf(args[1]).intValue() :
                PlatformCoreProperties.Values.REGISTRY_PORT;

        final ParametersPanel parameters = new ParametersPanel();
        parameters.addParameter(node, nodeArg);
        parameters.addParameter(port, "" + registryPortArg);

        final JFrame frame = new JFrame("ClearConnect | fimtra.com");
        frame.setIconImage(createIcon());
        frame.getContentPane().add(parameters);
        frame.getRootPane().setDefaultButton(parameters.ok);
        frame.pack();
        frame.addWindowListener(new WindowAdapter()
        {
            @Override
            public void windowClosing(WindowEvent e)
            {
                // happens when the user just closes the window without pressing OK
                super.windowClosing(e);
                System.exit(1);
            }
        });

        parameters.setOkButtonActionListener(e -> frame.dispose());

        frame.setVisible(true);

        SwingUtilities.invokeLater(() -> {
            parameters.parameters.get(node).value.requestFocusInWindow();
            parameters.parameters.get(node).value.selectAll();
        });

        Map<String, String> result = parameters.get();
        final PlatformMetaDataModel metaDatModel =
                new PlatformMetaDataModel(result.get(node), Integer.parseInt(result.get(port)));
        @SuppressWarnings("unused") PlatformDesktop desktop = new PlatformDesktop(metaDatModel);
    }

}
