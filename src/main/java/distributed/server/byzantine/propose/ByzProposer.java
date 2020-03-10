package distributed.server.byzantine.propose;

import distributed.server.byzantine.requests.SafeRequest;
import distributed.server.paxos.propose.Proposer;
import distributed.server.paxos.requests.AcceptRequest;
import distributed.server.paxos.requests.PrepareRequest;
import distributed.server.paxos.requests.Request;
import distributed.server.pojos.Server;
import distributed.server.pojos.WeightedResponse;
import distributed.server.threads.RequestThread;
import distributed.server.threads.WeightedRequestThread;
import distributed.utils.Command;
import distributed.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Byzantine proposer
 */
public class ByzProposer extends Proposer
{
    private static Logger logger = Logger.getLogger(ByzProposer.class);

    private void sendSafeRequest(SafeRequest safeRequest, List<Server> acceptors)
    {
        try
        {
            logger.debug("Sending safe request");
            sendRequest(safeRequest,acceptors,false);
        }catch (InterruptedException e)
        {
            logger.debug("Unable to send safe request",e);
        }
    }



    @Override
    protected void sendRequest(Request request, List<Server> acceptors, boolean waitForResponse) throws InterruptedException
    {
        // Execute broadcast in executor service
        ExecutorService executor = Executors.newFixedThreadPool(NUM_REQUEST_THREADS);
        List<Callable<WeightedResponse>> workers = new ArrayList<>();
        logger.debug("Executing send request pool: " + request + " waitForResponse: " + waitForResponse);
        for (Server acceptor : acceptors)
        {
            WeightedRequestThread worker = new WeightedRequestThread(request,acceptor,waitForResponse,acceptor.getWeight());
            workers.add(worker);
        }

        List<Future<WeightedResponse>> responses = executor.invokeAll(workers);
        for(Future<WeightedResponse> futureResponse : responses)
        {
            try
            {
                WeightedResponse response = futureResponse.get();
                parseResponseFromAcceptor(response.getResponse(),response.getWeight());

            }catch (Exception e)
            {
                logger.debug("Unable to process request to acceptor",e);
            }
        }

    }

    /**
     * Send the safe request to the acceptors to let them know a value is safe
     * @param acceptors
     * @return
     */
     
    @Override
    protected void sendAcceptRequest(AcceptRequest acceptRequest, List<Server> acceptors)
    {
        try
        {
            logger.debug("sending accept request");
            // Increment the number of accepts for ourself
            this.serverThread.getWeightedAccepts().set(this.serverThread.getWeightedAccepts().get() + this.serverThread.getOwnWeight().get());
            sendRequest(acceptRequest, acceptors);
        }catch (InterruptedException e)
        {
            logger.debug("Unable to send accept request",e);
        }
    }
    
    @Override
    protected void sendPrepareRequest(PrepareRequest prepareRequest, List<Server> acceptors)
    {
        try
        {
            logger.debug("sending prepare request");
            // Increment the number of promises for ourself
            this.serverThread.getWeightedPromises().set(this.serverThread.getWeightedPromises().get() + this.serverThread.getOwnWeight().get());
            sendRequest(prepareRequest, acceptors);
        }catch (InterruptedException e)
        {
            logger.debug("Unable to send prepare request",e);
        }
    }

    public boolean safe(List<Server> acceptors)
    {
        int id = this.serverThread.getPaxosId().get();
        String value =  this.serverThread.getPaxosValue();
        logger.debug("Sending safe for value " + value);
        SafeRequest safeRequest = new SafeRequest();
        safeRequest.setId(id);
        safeRequest.setValue(value);
        safeRequest.setSenderID(this.serverThread.getServerId());
        sendSafeRequest(safeRequest,acceptors);
        return true;
    }

    /**
     * Parse the responses from the acceptors
     * @param response
     */
    private void parseResponseFromAcceptor(String response, Float weight)
    {
        /**
         * TODO: If the request timed out, the input to this message is null, so neither accepts or rejects will update.
         * Does this cause issues?
         */
        logger.debug("Response from acceptor " + response);
        if(response != null)
        {
            String[] tokens = response.split("\\s+");
                if (Command.PROMISE.getCommand().equals(tokens[0]))
                {
                    // Update the id
                    updateIdAndValue(tokens);
                    // the prepare request was accepted
                    this.serverThread.updatePromisedWeight(weight);
                    logger.debug("Promised weight: " + this.serverThread.getWeightedPromises().get());

                }else if (Command.ACCEPT.getCommand().equals(tokens[0]))
                {
                    // the accept reqeust was accepted
                    updateId(tokens);
                    this.serverThread.updateAcceptedWeight(weight);
                    logger.debug("Accepted weight: " + this.serverThread.getWeightedAccepts().get());
                }else if(Command.REJECT_PREPARE.getCommand().equals(tokens[0]))
                {
                    // the prepare request was rejected
                    updateIdAndValue(tokens);
                    this.serverThread.updateWeightPromisesRejected(weight);
                    logger.debug("Promise rejected weight: " + this.serverThread.getWeightPromisesRejected().get());

                }else if(Command.REJECT_ACCEPT.getCommand().equals(tokens[0]))
                {
                    // The accept request was rejected
                    updateId(tokens);
                    this.serverThread.updateWeightAcceptsRejected(weight);
                    logger.debug("Rejected weight: " + this.serverThread.getWeightAcceptsRejected().get());
                }
        }
    }

}
