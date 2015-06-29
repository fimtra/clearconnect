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

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.GridLayout;
import java.awt.Image;
import java.awt.Point;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import javax.swing.border.Border;
import javax.swing.event.InternalFrameAdapter;
import javax.swing.event.InternalFrameEvent;

import com.fimtra.clearconnect.PlatformCoreProperties;
import com.fimtra.clearconnect.core.PlatformMetaDataModel.ServiceInstanceMetaDataRecordDefinition;
import com.fimtra.clearconnect.core.PlatformMetaDataModel.ServiceInstanceRpcMetaDataRecordDefinition;
import com.fimtra.clearconnect.core.PlatformMetaDataModel.ServiceProxyMetaDataRecordDefinition;
import com.fimtra.clearconnect.core.PlatformMetaDataModel.ServiceRpcMetaDataRecordDefinition;
import com.fimtra.clearconnect.event.IRegistryAvailableListener;
import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IRpcInstance;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.IValue.TypeEnum;
import com.fimtra.datafission.core.ContextUtils;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.ui.ColumnOrientedRecordTable;
import com.fimtra.datafission.ui.ColumnOrientedRecordTableModel;
import com.fimtra.datafission.ui.RowOrientedRecordTable;
import com.fimtra.datafission.ui.RowOrientedRecordTableModel;
import com.fimtra.tcpchannel.TcpChannelUtils;
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
class PlatformDesktop
{
    static final Executor executor =
        ThreadUtils.newSingleThreadExecutorService(ThreadUtils.getDirectCallingClassSimpleName());

    static Executor getExecutor()
    {
        return executor;
    }

    static abstract class AbstractPlatformDesktopView
    {
        final PlatformDesktop desktop;
        final JInternalFrame frame;

        AbstractPlatformDesktopView(String title, PlatformDesktop desktop)
        {
            super();
            this.desktop = desktop;
            this.frame = new JInternalFrame(title, true, true, true, true);
            this.frame.setSize(400, 200);
            this.frame.setVisible(true);
            this.frame.addInternalFrameListener(new InternalFrameAdapter()
            {
                @Override
                public void internalFrameClosed(InternalFrameEvent e)
                {
                    super.internalFrameClosed(e);
                    AbstractPlatformDesktopView.this.desktop.getViews().remove(AbstractPlatformDesktopView.this);
                    destroy();
                }
            });

            this.frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
            this.desktop.getViews().add(this);
            this.desktop.getDesktopPane().add(getFrame());
        }

        JInternalFrame getFrame()
        {
            return this.frame;
        }

        protected abstract void destroy();
    }

    static class RecordSubscriptionPlatformDesktopView extends AbstractPlatformDesktopView
    {
        final static int indexClass = 0;
        final static int indexX = 1;
        final static int indexY = 2;
        final static int indexWidth = 3;
        final static int indexHeight = 4;
        final static int indexTitle = 5;
        final static int indexViewKey = 6;
        final static int indexViewType = 7;
        final static int indexSubscriptions = 8;

        static RecordSubscriptionPlatformDesktopView fromStateString(PlatformDesktop desktop, String stateString)
        {
            final String[] tokens = stateString.split(",");

            String title = tokens[indexTitle];
            title = "null".equals(title.toLowerCase()) ? null : title;
            RecordSubscriptionPlatformDesktopView view =
                new RecordSubscriptionPlatformDesktopView(desktop, title,
                    PlatformMetaDataViewEnum.valueOf(tokens[indexViewType]), tokens[indexViewKey]);
            try
            {
                view.getFrame().setLocation(
                    new Point(Integer.parseInt(tokens[indexX]), Integer.parseInt(tokens[indexY])));
                view.getFrame().setSize(Integer.parseInt(tokens[indexWidth]), Integer.parseInt(tokens[indexHeight]));
            }
            catch (Exception e)
            {
                Log.log(PlatformDesktop.class, "Could not set position or size for '" + stateString + "'", e);
            }

            for (String recordSubscription : getSubscriptionsFromStateString(tokens[indexSubscriptions]))
            {
                view.subscribeFor(recordSubscription);
            }

            StringBuilder sb = new StringBuilder();
            final int start = indexSubscriptions + 1;
            for (int i = start; i < tokens.length; i++)
            {
                if (i > start)
                {
                    sb.append(",");
                }
                sb.append(tokens[i]);
            }
            view.table.fromStateString(sb.toString());

            return view;
        }

        static String toStateString(RecordSubscriptionPlatformDesktopView view)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(RecordSubscriptionPlatformDesktopView.class.getSimpleName()).append(","); // 0
            sb.append(view.getFrame().getX()).append(","); // 1
            sb.append(view.getFrame().getY()).append(","); // 2
            sb.append(view.getFrame().getWidth()).append(","); // 3
            sb.append(view.getFrame().getHeight()).append(","); // 4
            sb.append(view.title).append(","); // 5
            sb.append(view.metaDataViewKey).append(","); // 6
            sb.append(view.metaDataViewType).append(","); // 7
            sb.append(getSubscriptionsStateString(view)).append(","); // 8
            sb.append(view.table.toStateString()); // the rest
            return sb.toString();
        }

        private static String getSubscriptionsStateString(RecordSubscriptionPlatformDesktopView view)
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
            List<String> subscriptions = new ArrayList<String>(tokens.length);
            for (String string : tokens)
            {
                subscriptions.add(string.replace("\\|", "|"));
            }
            return subscriptions;
        }

        final String title;
        final ColumnOrientedRecordTable table;
        final ColumnOrientedRecordTableModel model;
        final List<String> subscribedRecords;
        final String metaDataViewKey;
        final PlatformMetaDataViewEnum metaDataViewType;
        final IObserverContext context;

        public RecordSubscriptionPlatformDesktopView(PlatformDesktop desktop, String title,
            PlatformMetaDataViewEnum metaDataViewType, String metaDataViewKey)
        {
            super(title + " : " + metaDataViewKey, desktop);
            this.title = title;
            this.metaDataViewKey = metaDataViewKey;
            this.metaDataViewType = metaDataViewType;

            this.model = new ColumnOrientedRecordTableModel();
            this.table = new ColumnOrientedRecordTable(this.model);
            // metaDataViewType will either be RECORDS_PER_SERVICE or RECORDS_PER_INSTANCE and the
            // metaDataViewKey will then either be the serviceFamily or serviceMember
            switch(metaDataViewType)
            {
                case RECORDS_PER_INSTANCE:
                    this.context =
                        desktop.getMetaDataModel().getProxyContextForPlatformServiceInstance(metaDataViewKey);
                    break;
                case RECORDS_PER_SERVICE:
                    this.context = desktop.getMetaDataModel().getProxyContextForPlatformService(metaDataViewKey);
                    break;
                default :
                    throw new IllegalStateException("Unsupported: " + metaDataViewType);
            }

            this.subscribedRecords = new CopyOnWriteArrayList<String>();
            this.model.addRecordRemovedListener(this.context);

            prepareTablePopupMenu();

            this.frame.add(new JScrollPane(this.table));
            synchronized (PlatformMetaDataViewEnum.recordSubscriptionViews)
            {
                PlatformMetaDataViewEnum.recordSubscriptionViews.put(this.metaDataViewKey, this);
            }
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
                    final IRecord selectedRecord = RecordSubscriptionPlatformDesktopView.this.table.getSelectedRecord();
                    unsubscribeFor(selectedRecord.getName());
                    RecordSubscriptionPlatformDesktopView.this.table.getModel().recordUnsubscribed(selectedRecord);
                }
            });
            menuItem.setText("Remove");
            popupMenu.add(menuItem);
            this.table.setComponentPopupMenu(popupMenu);
        }

        @Override
        protected void destroy()
        {

            this.context.removeObserver(this.model,
                this.subscribedRecords.toArray(new String[this.subscribedRecords.size()]));
            synchronized (PlatformMetaDataViewEnum.recordSubscriptionViews)
            {
                PlatformMetaDataViewEnum.recordSubscriptionViews.remove(this.metaDataViewKey);
            }
            this.model.removeRecordRemovedListener(this.context);
            PlatformMetaDataViewEnum.deregister(this.context, this.model);
        }

        void subscribeFor(String recordNameToSubscribe)
        {
            if (this.subscribedRecords.add(recordNameToSubscribe))
            {
                this.context.addObserver(this.model, recordNameToSubscribe);
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

    static class RpcPlatformDesktopView extends AbstractPlatformDesktopView
    {
        RpcPlatformDesktopView(PlatformDesktop desktop, String title, IRecord rpcRecordDefinition,
            final PlatformMetaDataViewEnum parentMetaDataViewType)
        {
            super(title + " : " + rpcRecordDefinition.getContextName() + "." + rpcRecordDefinition.getName(), desktop);
            final String rpcName = rpcRecordDefinition.getName();
            final String contextName = rpcRecordDefinition.getContextName();

            final IRpcInstance instance;
            switch(parentMetaDataViewType)
            {
                case RPCS_PER_INSTANCE:
                    instance =
                        RpcInstance.constructInstanceFromDefinition(
                            rpcName,
                            rpcRecordDefinition.get(ServiceInstanceRpcMetaDataRecordDefinition.Definition.toString()).textValue());
                    break;
                case RPCS_PER_SERVICE:
                    instance =
                        RpcInstance.constructInstanceFromDefinition(
                            rpcName,
                            rpcRecordDefinition.get(ServiceRpcMetaDataRecordDefinition.Definition.toString()).textValue());
                    break;
                default :
                    throw new IllegalStateException("Unsupported: " + parentMetaDataViewType);
            }

            final JTextField result = new JTextField("No result");
            result.setEditable(false);

            final ParametersPanel parameters = new ParametersPanel();
            parameters.setOkButtonActionListener(new ActionListener()
            {
                @Override
                public void actionPerformed(ActionEvent e)
                {
                    result.setText("Executing...");
                    parameters.setEnabled(false);

                    getExecutor().execute(new Runnable()
                    {
                        @Override
                        public void run()
                        {
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
                                    final IObserverContext proxyContext;
                                    final String textValue;
                                    switch(parentMetaDataViewType)
                                    {
                                        case RPCS_PER_INSTANCE:
                                            proxyContext =
                                                RpcPlatformDesktopView.this.desktop.getMetaDataModel().getProxyContextForPlatformServiceInstance(
                                                    contextName);
                                            break;
                                        case RPCS_PER_SERVICE:
                                            proxyContext =
                                                RpcPlatformDesktopView.this.desktop.getMetaDataModel().getProxyContextForPlatformService(
                                                    contextName);
                                            break;
                                        default :
                                            throw new IllegalStateException("Unsupported: " + parentMetaDataViewType);
                                    }
                                    final IValue executeRpcResult =
                                        RpcPlatformDesktopView.this.desktop.getMetaDataModel().executeRpc(proxyContext,
                                            rpcName, rpcArgs);
                                    if (executeRpcResult != null)
                                    {
                                        textValue = executeRpcResult.textValue();
                                    }
                                    else
                                    {
                                        textValue = null;
                                    }
                                    SwingUtilities.invokeLater(new Runnable()
                                    {
                                        @Override
                                        public void run()
                                        {
                                            result.setText(textValue);
                                        }
                                    });
                                }
                                catch (final Exception e1)
                                {
                                    SwingUtilities.invokeLater(new Runnable()
                                    {
                                        @Override
                                        public void run()
                                        {
                                            String message = e1.getMessage();
                                            if (message == null || message.length() == 0)
                                            {
                                                message = e1.getClass().getSimpleName();
                                            }
                                            result.setText(message);
                                        }
                                    });
                                }
                            }
                            finally
                            {
                                SwingUtilities.invokeLater(new Runnable()
                                {
                                    @Override
                                    public void run()
                                    {
                                        parameters.setEnabled(true);
                                    }
                                });
                            }
                        }
                    });
                }
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
            this.frame.pack();
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
        protected void destroy()
        {
        }
    }

    static class MetaDataPlatformDesktopView extends AbstractPlatformDesktopView
    {
        final static int indexClass = 0;
        final static int indexX = 1;
        final static int indexY = 2;
        final static int indexWidth = 3;
        final static int indexHeight = 4;
        final static int indexTitle = 5;
        final static int indexViewKey = 6;
        final static int indexViewType = 7;

        static MetaDataPlatformDesktopView fromStateString(PlatformDesktop desktop, String stateString)
        {
            final String[] tokens = stateString.split(",");

            String title = tokens[indexTitle];
            title = "null".equals(title.toLowerCase()) ? null : title;
            String viewKey = tokens[indexViewKey];
            viewKey = "null".equals(viewKey.toLowerCase()) ? null : viewKey;
            MetaDataPlatformDesktopView view =
                new MetaDataPlatformDesktopView(desktop, title,
                    PlatformMetaDataViewEnum.valueOf(tokens[indexViewType]), viewKey);
            try
            {
                view.getFrame().setLocation(
                    new Point(Integer.parseInt(tokens[indexX]), Integer.parseInt(tokens[indexY])));
                view.getFrame().setSize(Integer.parseInt(tokens[indexWidth]), Integer.parseInt(tokens[indexHeight]));
            }
            catch (Exception e)
            {
                Log.log(PlatformDesktop.class, "Could not set position or size for '" + stateString + "'", e);
            }

            StringBuilder sb = new StringBuilder();
            final int start = indexViewType + 1;
            for (int i = start; i < tokens.length; i++)
            {
                if (i > start)
                {
                    sb.append(",");
                }
                sb.append(tokens[i]);
            }
            view.table.fromStateString(sb.toString());

            return view;
        }

        static String toStateString(MetaDataPlatformDesktopView view)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(MetaDataPlatformDesktopView.class.getSimpleName()).append(","); // 0
            sb.append(view.getFrame().getX()).append(","); // 1
            sb.append(view.getFrame().getY()).append(","); // 2
            sb.append(view.getFrame().getWidth()).append(","); // 3
            sb.append(view.getFrame().getHeight()).append(","); // 4
            sb.append(view.title).append(","); // 5
            sb.append(view.metaDataViewKey).append(","); // 6
            sb.append(view.metaDataViewType).append(","); // 7
            sb.append(view.table.toStateString()); // the rest
            return sb.toString();
        }

        final String title;
        final RowOrientedRecordTable table;
        final RowOrientedRecordTableModel model;
        final String metaDataViewKey;
        final PlatformMetaDataViewEnum metaDataViewType;
        final IObserverContext context;

        MetaDataPlatformDesktopView(PlatformDesktop platformDesktop, String title,
            PlatformMetaDataViewEnum metaDataViewType, String metaDataViewKey)
        {
            super(title + ((metaDataViewKey == null || metaDataViewKey.isEmpty()) ? "" : " : " + metaDataViewKey),
                platformDesktop);
            this.title = title;
            this.metaDataViewKey = metaDataViewKey;
            this.metaDataViewType = metaDataViewType;

            this.model = new RowOrientedRecordTableModel();
            this.table = new RowOrientedRecordTable(this.model);
            this.context =
                metaDataViewType.getContextForMetaDataViewType(platformDesktop.getMetaDataModel(), this.metaDataViewKey);
            this.metaDataViewType.register(this.context, this.model, this.metaDataViewKey);
            this.model.addRecordRemovedListener(this.context);

            prepareTablePopupMenu();

            this.frame.add(new JScrollPane(this.table));
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
        protected void destroy()
        {
            this.model.removeRecordRemovedListener(this.context);
            PlatformMetaDataViewEnum.deregister(this.context, this.model);
        }

        IRecord getSelectedRecord()
        {
            return this.table.getSelectedRecord();
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
            this.parameters = new LinkedHashMap<String, Parameter>();
            this.parametersPanel = new JPanel(new GridLayout(0, 2));
            this.result = new AtomicReference<LinkedHashMap<String, String>>();

            this.ok.addActionListener(new ActionListener()
            {
                @Override
                public void actionPerformed(ActionEvent e)
                {
                    LinkedHashMap<String, String> values = new LinkedHashMap<String, String>();
                    Map.Entry<String, Parameter> entry = null;
                    String key = null;
                    Parameter value = null;
                    for (Iterator<Map.Entry<String, Parameter>> it =
                        ParametersPanel.this.parameters.entrySet().iterator(); it.hasNext();)
                    {
                        entry = it.next();
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
    static enum PlatformMetaDataViewEnum
    {
            // data views
            RPC(RpcPlatformDesktopView.class, "", null),

            RECORD(RecordSubscriptionPlatformDesktopView.class, null, null),

            RPCS_PER_SERVICE(MetaDataPlatformDesktopView.class, null, null, RPC),

            RECORDS_PER_SERVICE(MetaDataPlatformDesktopView.class, null, PlatformMetaDataModel.RECORD_NAME_FIELD,
                RECORD),

            RPCS_PER_INSTANCE(MetaDataPlatformDesktopView.class, null, null, RPC),

            RECORDS_PER_INSTANCE(MetaDataPlatformDesktopView.class, null, PlatformMetaDataModel.RECORD_NAME_FIELD,
                RECORD),

            CLIENTS_PER_SERVICE(MetaDataPlatformDesktopView.class,
                ServiceProxyMetaDataRecordDefinition.Service.toString(), null),

            CLIENTS_PER_INSTANCE(MetaDataPlatformDesktopView.class,
                ServiceProxyMetaDataRecordDefinition.ServiceInstance.toString(), null),

            // grouping views
            INSTANCES_PER_SERVICE(MetaDataPlatformDesktopView.class,
                ServiceInstanceMetaDataRecordDefinition.Service.toString(), PlatformMetaDataModel.RECORD_NAME_FIELD,
                CLIENTS_PER_INSTANCE, RECORDS_PER_INSTANCE, RPCS_PER_INSTANCE),

            INSTANCES_PER_NODE(MetaDataPlatformDesktopView.class,
                ServiceInstanceMetaDataRecordDefinition.Node.toString(), PlatformMetaDataModel.RECORD_NAME_FIELD,
                CLIENTS_PER_INSTANCE, RECORDS_PER_INSTANCE, RPCS_PER_INSTANCE),

            // the main views
            AGENTS(MetaDataPlatformDesktopView.class, null, null),

            CONNECTIONS(MetaDataPlatformDesktopView.class, null, null),

            SERVICES(MetaDataPlatformDesktopView.class, null, PlatformMetaDataModel.RECORD_NAME_FIELD,
                CLIENTS_PER_SERVICE, INSTANCES_PER_SERVICE, RECORDS_PER_SERVICE, RPCS_PER_SERVICE),

            NODES(MetaDataPlatformDesktopView.class, null, PlatformMetaDataModel.RECORD_NAME_FIELD, INSTANCES_PER_NODE),

        ;

        static final Map<String, RecordSubscriptionPlatformDesktopView> recordSubscriptionViews =
            new HashMap<String, RecordSubscriptionPlatformDesktopView>();

        static void deregister(IObserverContext context, IRecordListener observer)
        {
            final Set<String> recordNames = context.getRecordNames();
            for (String recordName : recordNames)
            {
                context.removeObserver(observer, recordName);
            }
        }

        final PlatformMetaDataViewEnum[] childViews;
        /** The key to link with the parent's child view key */
        final String parentViewKeyField;
        /** The key to link with child view's parent key */
        final String childViewKeyField;
        final Class<? extends AbstractPlatformDesktopView> viewClass;

        PlatformMetaDataViewEnum(Class<? extends AbstractPlatformDesktopView> viewClass, String parentViewKeyField,
            String childViewKeyField, PlatformMetaDataViewEnum... childViews)
        {
            this.viewClass = viewClass;
            this.parentViewKeyField = parentViewKeyField;
            this.childViewKeyField = childViewKeyField;
            this.childViews = childViews;
        }

        Action createActionToOpenChildView(final PlatformDesktop desktop, final MetaDataPlatformDesktopView parentTable)
        {
            return new AbstractAction("Show " + PlatformMetaDataViewEnum.this)
            {
                private static final long serialVersionUID = 1L;

                @SuppressWarnings("unused")
                @Override
                public void actionPerformed(ActionEvent e)
                {
                    final String title = PlatformMetaDataViewEnum.this.toString();

                    if (PlatformMetaDataViewEnum.this.viewClass == MetaDataPlatformDesktopView.class)
                    {
                        final String viewKey;
                        if (parentTable != null)
                        {
                            final IRecord selectedRecord = parentTable.getSelectedRecord();
                            // our viewKeyField is the parent's child viewKey
                            final String viewKeyField = parentTable.metaDataViewType.childViewKeyField;
                            if (PlatformMetaDataModel.RECORD_NAME_FIELD.equals(viewKeyField))
                            {
                                viewKey = selectedRecord.getName();
                            }
                            else
                            {
                                viewKey = selectedRecord.get(viewKeyField).textValue();
                            }
                        }
                        else
                        {
                            viewKey = null;
                        }
                        boolean create = true;
                        for (AbstractPlatformDesktopView view : desktop.views)
                        {
                            if (view instanceof MetaDataPlatformDesktopView)
                            {
                                MetaDataPlatformDesktopView other = (MetaDataPlatformDesktopView) view;
                                if (is.eq(title, other.title) && is.eq(viewKey, other.metaDataViewKey)
                                    && is.eq(PlatformMetaDataViewEnum.this, other.metaDataViewType))
                                {
                                    other.frame.toFront();
                                    final Border previousBorder = other.frame.getBorder();
                                    other.frame.setBorder(BorderFactory.createLineBorder(Color.RED, 4));
                                    final MetaDataPlatformDesktopView duplicate = other;
                                    ThreadUtils.newThread(new Runnable()
                                    {
                                        @Override
                                        public void run()
                                        {
                                            try
                                            {
                                                Thread.sleep(500);
                                            }
                                            catch (InterruptedException e)
                                            {
                                            }
                                            SwingUtilities.invokeLater(new Runnable()
                                            {
                                                @Override
                                                public void run()
                                                {
                                                    duplicate.frame.setBorder(previousBorder);
                                                }
                                            });
                                        }
                                    }, MetaDataPlatformDesktopView.class.getSimpleName() + "-highlighter").start();
                                    create = false;
                                    break;
                                }
                            }
                        }
                        if (create)
                        {
                            new MetaDataPlatformDesktopView(desktop, title, PlatformMetaDataViewEnum.this, viewKey);
                        }
                    }
                    else
                    {
                        if (PlatformMetaDataViewEnum.this.viewClass == RecordSubscriptionPlatformDesktopView.class)
                        {
                            synchronized (recordSubscriptionViews)
                            {
                                final String nameOfServiceOrServiceInstance = parentTable.metaDataViewKey;
                                RecordSubscriptionPlatformDesktopView view =
                                    recordSubscriptionViews.get(nameOfServiceOrServiceInstance);
                                if (view == null)
                                {
                                    view =
                                    // NOTE: the constructor registers with the
                                    // recordSubscriptionViews
                                        new RecordSubscriptionPlatformDesktopView(desktop, title,
                                            parentTable.metaDataViewType, nameOfServiceOrServiceInstance);
                                }
                                view.subscribeFor(parentTable.getSelectedRecord().getName());
                            }
                        }
                        else
                        {
                            if (PlatformMetaDataViewEnum.this.viewClass == RpcPlatformDesktopView.class)
                            {
                                final IRecord selectedRecord = parentTable.getSelectedRecord();
                                new RpcPlatformDesktopView(desktop, title, selectedRecord, parentTable.metaDataViewType);
                            }
                        }
                    }
                }
            };
        }

        PlatformMetaDataViewEnum[] getChildViews()
        {
            return this.childViews;
        }

        void register(final IObserverContext context, final IRecordListener observer, final String parentViewKey)
        {
            // listen for all records (added and removed)
            final IRecordListener listener = new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    if (PlatformMetaDataViewEnum.this.parentViewKeyField == null || parentViewKey == null)
                    {
                        observer.onChange(imageCopy, atomicChange);
                        return;
                    }

                    IValue recordParentViewKey = imageCopy.get(PlatformMetaDataViewEnum.this.parentViewKeyField);
                    if (recordParentViewKey != null && parentViewKey.equals(recordParentViewKey.textValue()))
                    {
                        observer.onChange(imageCopy, atomicChange);
                    }
                }
            };
            ContextUtils.addAllRecordsListener(context, listener);
        }

        IObserverContext getContextForMetaDataViewType(PlatformMetaDataModel model, String contextKey)
        {
            switch(this)
            {
                case CONNECTIONS:
                    return model.getPlatformConnectionsContext();
                case AGENTS:
                    return model.getPlatformRegsitryAgentsContext();
                case CLIENTS_PER_INSTANCE:
                case CLIENTS_PER_SERVICE:
                    return model.getPlatformServiceProxiesContext();
                case NODES:
                    return model.getPlatformNodesContext();
                case SERVICES:
                    return model.getPlatformServicesContext();
                case INSTANCES_PER_NODE:
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
                default :
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
            platformDesktop.getDesktopWindow().setSize(Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]));
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
        this.views = new HashSet<AbstractPlatformDesktopView>();
        SwingUtilities.invokeLater(new Runnable()
        {
            @Override
            public void run()
            {
                PlatformDesktop.this.desktopPane = new JDesktopPane();
                PlatformDesktop.this.desktopWindow = new JFrame();
				PlatformDesktop.this.desktopWindow.setIconImage(createIcon());

                platformMetaDataModel.agent.addRegistryAvailableListener(new IRegistryAvailableListener()
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
                        this.platformName = PlatformDesktop.this.getMetaDataModel().agent.getPlatformName();
						setTitle(this.platformName + " [CONNECTED]");
                        PlatformDesktop.this.desktopPane.setEnabled(true);
                    }

					private void setTitle(String title) {
						PlatformDesktop.this.desktopWindow.setTitle("ClearConnect | " + title + " | fimtra.com");
					}
                });

                PlatformDesktop.this.getDesktopWindow().addWindowListener(new WindowAdapter()
                {
                    @Override
                    public void windowClosing(WindowEvent e)
                    {
                        super.windowClosing(e);
                        saveDesktopPlatformViewState();
                    }
                });
                PlatformDesktop.this.getDesktopWindow().setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
                PlatformDesktop.this.getDesktopWindow().getContentPane().add(PlatformDesktop.this.getDesktopPane(),
                    BorderLayout.CENTER);
                PlatformDesktop.this.getDesktopWindow().setSize(640, 480);

                JPopupMenu popupMenu = new JPopupMenu();
                for (final PlatformMetaDataViewEnum type : new PlatformMetaDataViewEnum[] {
                    PlatformMetaDataViewEnum.CONNECTIONS, PlatformMetaDataViewEnum.NODES,
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
                    if (view instanceof MetaDataPlatformDesktopView)
                    {
                        pw.println(MetaDataPlatformDesktopView.toStateString((MetaDataPlatformDesktopView) view));
                    }
                    else
                    {
                        if (view instanceof RecordSubscriptionPlatformDesktopView)
                        {
                            pw.println(RecordSubscriptionPlatformDesktopView.toStateString((RecordSubscriptionPlatformDesktopView) view));
                        }
                    }
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
        try
        {
            File stateFile = new File(getStateFileName());
            if (stateFile.exists())
            {
                BufferedReader br = new BufferedReader(new FileReader(stateFile));
                if (br.ready())
                {
                    fromStateString(this, br.readLine());
                    String line;
                    while (br.ready())
                    {
                        line = br.readLine();
                        if (line.startsWith(MetaDataPlatformDesktopView.class.getSimpleName()))
                        {
                            MetaDataPlatformDesktopView.fromStateString(this, line);
                        }
                        else
                        {
                            if (line.startsWith(RecordSubscriptionPlatformDesktopView.class.getSimpleName()))
                            {
                                RecordSubscriptionPlatformDesktopView.fromStateString(this, line);
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not read state", e);
        }
    }

	private static Image createIcon() {
		String hexIcon = "89504E470D0A1A0A0000000D4948445200000020000000200806000000737A7AF400000006624B474400FF00FF00FFA0BDA793000000097048597300000B1300000B1301009A9C180000000774494D4507D"
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
				+ "E4ECEA6FCFCFCC3344D8B04412084109E104200F00008C7712E93C954E07038261FBE31FF77F91B46228F9FE19DE6B40000000049454E44AE426082";

		int len = hexIcon.length();
		byte[] imgData = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			imgData[i / 2] = (byte) ((Character.digit(hexIcon.charAt(i), 16) << 4) + Character.digit(hexIcon.charAt(i + 1), 16));
		}
		return new ImageIcon(imgData).getImage();
	}

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

        final String node = "Platform node";
        final String port = "platform port";

        ParametersPanel parameters = new ParametersPanel();
        parameters.addParameter(node, TcpChannelUtils.LOOPBACK);
        parameters.addParameter(port, "" + PlatformCoreProperties.Values.REGISTRY_PORT);

		final JFrame frame = new JFrame("ClearConnect | fimtra.com");
		frame.setIconImage(createIcon());
        frame.getContentPane().add(parameters);
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

        parameters.setOkButtonActionListener(new ActionListener()
        {
            @Override
            public void actionPerformed(ActionEvent e)
            {
                frame.dispose();
            }
        });

        frame.setVisible(true);
        parameters.ok.requestFocusInWindow();

        Map<String, String> result = parameters.get();
        final PlatformMetaDataModel metaDatModel =
            new PlatformMetaDataModel(result.get(node), Integer.parseInt(result.get(port)));
        @SuppressWarnings("unused")
        PlatformDesktop desktop = new PlatformDesktop(metaDatModel);
    }

}
