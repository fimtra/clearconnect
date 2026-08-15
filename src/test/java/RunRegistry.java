import com.fimtra.clearconnect.core.PlatformRegistry;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.util.ThreadUtils;

/**
 * @author Ramon Servadei
 */
public class RunRegistry
{
    public static void main(String[] args) throws InterruptedException
    {
        System.setProperty("tcpChannel.serverSuspiciousConnectionGracePeriodMillis", "6000000");
        System.setProperty("tcpChannel.slsMinSocketAliveTimeMillis", "1");
        new PlatformRegistry("burnin test", TcpChannelUtils.LOCALHOST_IP);
        synchronized ("")
        {
            "".wait();
        }
    }
}
