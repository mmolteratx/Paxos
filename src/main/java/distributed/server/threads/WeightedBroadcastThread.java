package distributed.server.threads;

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
public class WeightedBroadcastThread implements Callable<WeightedResponse>
{
    private Server acceptor;
    private String command;
    private boolean waitForResponse;
    private Float weight;

    private static Logger logger = Logger.getLogger(WeightedBroadcastThread.class);

    public WeightedResponse call() throws InvalidParameterException
    {
        logger.debug("Sending request " + command + " to acceptor" + acceptor);
        // Send the command over TCP
        String response = Utils.sendTcpMessage(acceptor, command, waitForResponse);
        WeightedResponse weightedResponse = new WeightedResponse(response,this.weight);
        return weightedResponse;
    }


}
