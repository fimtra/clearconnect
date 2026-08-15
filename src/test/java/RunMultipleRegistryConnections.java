import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.clearconnect.RedundancyModeEnum;
import com.fimtra.clearconnect.WireProtocolEnum;
import com.fimtra.clearconnect.core.PlatformRegistryAgent;
import com.fimtra.clearconnect.event.IServiceAvailableListener;
import com.fimtra.tcpchannel.TcpChannelUtils;

/**
 * @author Ramon Servadei
 */
public class RunMultipleRegistryConnections
{
    public static void main(String[] args) throws IOException, InterruptedException
    {
        int loop = 0;
        while (true)
        {
            System.err.println("loop " + loop);
            final int max = 100;
            System.err.println("createAgentsAndServicesThenDestroy " + max);
            createAgentsAndServicesThenDestroy(loop++, max);

            // todo 1 run only to find out why destroy takes so long
            //            if (true)
            //            {
            //                break;
            //            }
            wait5();
            System.gc();
        }
    }

    private static void createAgentsAndServicesThenDestroy(final int loop, int MAX)
            throws IOException, InterruptedException
    {
        final AtomicReference<CountDownLatch> connectedLatch = new AtomicReference<>();
        connectedLatch.set(new CountDownLatch(MAX));
        final PlatformRegistryAgent[] agents = new PlatformRegistryAgent[MAX];
        final long createStart = System.currentTimeMillis();
        for (int i = 0; i < MAX; i++)
        {
            try
            {
                final String suffix = "" + i;
                agents[i] = new PlatformRegistryAgent("Test-Agent-" + loop + "-" + suffix,
                        TcpChannelUtils.LOCALHOST_IP);
                final PlatformRegistryAgent agent = agents[i];
                System.err.println("Constructed " + loop + " " + agent);
                agent.setRegistryReconnectPeriodMillis(500);

                final String _serviceFamily = "svcFamily-" + suffix;
                agent.addServiceAvailableListener(new IServiceAvailableListener()
                {
                    @Override
                    public void onServiceAvailable(String serviceFamily)
                    {
                        if (_serviceFamily.equals(serviceFamily))
                        {
                            connectedLatch.get()
                                    .countDown();
                        }
                    }

                    @Override
                    public void onServiceUnavailable(String serviceFamily)
                    {

                    }
                });
                agent.createPlatformServiceInstance(_serviceFamily, "sdf", //loop + "-" + suffix,
                        TcpChannelUtils.LOCALHOST_IP, WireProtocolEnum.GZIP,
                        RedundancyModeEnum.FAULT_TOLERANT);

            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        System.err.println("Took " + (System.currentTimeMillis() - createStart) + "ms to create " + MAX
                + " agents and services");

        System.err.println("Checking all connected");
        if (!connectedLatch.get()
                .await(5, TimeUnit.SECONDS))
        {
            System.err.println("Did not connect");
            System.exit(234);
        }
        wait5();
        System.err.println("Destroying agents + services");
        final AtomicReference<CountDownLatch> destroyedCount = new AtomicReference<>();
        destroyedCount.set(new CountDownLatch(MAX));

        final long destroyStart = System.currentTimeMillis();
        for (final PlatformRegistryAgent agent : agents)
        {
            final long _destroyStart = System.currentTimeMillis();
            agent.destroy();
            System.err.println(
                    "Took " + (System.currentTimeMillis() - _destroyStart) + " to destroy " + agent);
            destroyedCount.get()
                    .countDown();
        }
        System.err.println(
                "Took " + (System.currentTimeMillis() - destroyStart) + "ms to destroy " + MAX + " agents");
        System.err.println("Checking agents destroyed");
        if (!connectedLatch.get()
                .await(5, TimeUnit.SECONDS))
        {
            System.err.println("Did not get signalled for half disconnected");
            System.exit(234);
        }
        else
        {
            System.err.println("END CYCLE " + loop + " =======================");
        }
    }

    private static void wait5() throws InterruptedException
    {
        System.err.println("Waiting 5 secs...");
        Thread.sleep(5000);
    }
}
