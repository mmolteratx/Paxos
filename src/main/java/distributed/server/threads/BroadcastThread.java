package distributed.server.threads;

import distributed.server.pojos.Server;
import distributed.utils.Command;
import distributed.utils.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.security.InvalidParameterException;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;

@Data
@AllArgsConstructor
public class BroadcastThread implements Callable<String>
{
    private Server acceptor;
    private String command;
    private boolean waitForResponse;

    private static Logger logger = Logger.getLogger(BroadcastThread.class);

    public String call() throws InvalidParameterException
    {
        String response = Utils.sendTcpMessage(this.acceptor, this.command,this.waitForResponse);
        return response;
    }

}
