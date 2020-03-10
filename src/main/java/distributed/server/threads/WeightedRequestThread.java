package distributed.server.threads;

import distributed.server.paxos.requests.Request;
import distributed.server.pojos.Server;
import distributed.server.pojos.WeightedResponse;
import distributed.utils.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.concurrent.Callable;

@Data
@AllArgsConstructor
public class WeightedRequestThread implements Callable<WeightedResponse>
{
    private Request request;
    private Server acceptor;
    private boolean waitForResponse;
    private Float weight;

    private static Logger logger = Logger.getLogger(WeightedRequestThread.class);

    public WeightedResponse call() throws InvalidParameterException
    {
        logger.debug("Sending request " + request.toString() + " to acceptor" + acceptor);
        String command = request.toString();
        // Send the command over TCP
        String response = Utils.sendTcpMessage(acceptor, command, waitForResponse);
        WeightedResponse weightedResponse = new WeightedResponse(response,this.weight);
        return weightedResponse;
    }

}
