package distributed.server.threads;

import distributed.server.paxos.requests.Request;
import distributed.server.pojos.Server;
import distributed.utils.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.concurrent.Callable;

@Data
@AllArgsConstructor
public class RequestThread implements Callable<String>
{
    private Request request;
    private Server acceptor;
    private boolean waitForResponse;

    private static Logger logger = Logger.getLogger(RequestThread.class);

    public String call() throws InvalidParameterException
    {
        logger.debug("Sending request " + request.toString() + " to acceptor" + acceptor);
        String command = request.toString();
        // Send the command over TCP
        String response = Utils.sendTcpMessage(acceptor, command, waitForResponse);
        return response;
    }
}
