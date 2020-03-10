package distributed.server.paxos.propose;

import distributed.server.pojos.Server;
import distributed.server.paxos.requests.AcceptRequest;
import distributed.server.paxos.requests.PrepareRequest;
import distributed.server.paxos.requests.Request;
import distributed.server.threads.RequestThread;
import distributed.server.threads.ServerThread;
import distributed.utils.Command;
import distributed.utils.Utils;
import lombok.Data;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Data
public class Proposer
{
    private static Logger logger = Logger.getLogger(Proposer.class);
    protected ServerThread serverThread;

    protected  static int NUM_REQUEST_THREADS = 5;


    /**
     * Parse the responses from the acceptors
     * @param response
     */
    private void parseResponseFromAcceptor(String response)
    {
        /**
         * TODO: If the request timed out, the input to this message is null, so neither accepts or rejects will update.
         * Does this cause issues?
         */
        logger.debug("Response from acceptor " + response);
        if(response != null)
        {
            String[] tokens = response.split("\\s+");
            if(tokens.length > 1)
            {
                if (Command.PROMISE.getCommand().equals(tokens[0]))
                {

                    // Update the id
                    updateIdAndValue(tokens);
                    // the prepare request was accepted
                    this.serverThread.incrementNumPromises();

                }else if (Command.ACCEPT.getCommand().equals(tokens[0]))
                {
                    // the accept reqeust was accepted
                    updateId(tokens);
                    this.serverThread.incrementNumAccepts();
                }else if(Command.REJECT_PREPARE.getCommand().equals(tokens[0]))
                {
                    // the prepare request was rejected
                    updateIdAndValue(tokens);
                    this.serverThread.incrementNumPromisesRejected();

                }else if(Command.REJECT_ACCEPT.getCommand().equals(tokens[0]))
                {
                    // The accept request was rejected
                    updateId(tokens);
                    this.serverThread.incrementNumAcceptsRejected();
                }
            }

        }
    }
    
    // TODO: Verify ID and value update; still not 100% sure this is correct. A value will be chosen but I don't think it
    // will always be the correct value
    protected void updateId(String[] tokens)
    {
        // Update the id
        if (tokens.length > 2)
        {
            int id = Integer.parseInt(tokens[1]);
            if (id > this.serverThread.getPaxosId().get())
            {
                this.serverThread.getPaxosId().set(id);
            }
        }
    }

    protected void updateIdAndValue(String[] tokens)
    {
        // Update the id
        if (tokens.length > 3)
        {
            int id = Integer.parseInt(tokens[1]);
            String value = tokens[2];
            if (id > this.serverThread.getPaxosId().get())
            {
                this.serverThread.getPaxosId().set(id);
                this.serverThread.setPaxosValue(value);
            }
        }
    }



    /**
     * Send the request to the acceptors
     *
     * @param acceptors
     * @return
     */
    protected void sendRequest(Request request, List<Server> acceptors, boolean waitForResponse) throws InterruptedException
    {
        // Execute broadcast in executor service
        ExecutorService executor = Executors.newFixedThreadPool(NUM_REQUEST_THREADS);
        List<Callable<String>> workers = new ArrayList<>();
        logger.debug("Executing send request pool: " + request + " waitForResponse: " + waitForResponse);
        for (Server acceptor : acceptors)
        {
            RequestThread worker = new RequestThread(request,acceptor,waitForResponse);
            workers.add(worker);
        }

        List<Future<String>> responses = executor.invokeAll(workers);
        for(Future<String> futureResponse : responses)
        {
            try
            {
                String response = futureResponse.get();
                parseResponseFromAcceptor(response);

            }catch (Exception e)
            {
                logger.debug("Unable to process request to acceptor",e);
            }
        }

    }

    protected void sendRequest(Request request, List<Server> acceptors) throws InterruptedException
    {
        sendRequest(request,acceptors,true);
    }

    public boolean accept(List<Server> acceptors)
    {
        int id = this.serverThread.getPaxosId().get();
        String value =  this.serverThread.getPaxosValue();
        AcceptRequest acceptRequest = new AcceptRequest();
        acceptRequest.setId(id);
        acceptRequest.setValue(value);
        // send the accept request to all acceptors
        sendAcceptRequest(acceptRequest,acceptors);
        return true;
    }

    /**
     * Send the accept request to the servers
     * @param acceptRequest
     * @param acceptors
     */
    protected void sendAcceptRequest(AcceptRequest acceptRequest, List<Server> acceptors)
    {
        try
        {
            logger.debug("Sending accept request");
            sendRequest(acceptRequest, acceptors);
            // Increment the number of accepts for ourself
            this.serverThread.incrementNumAccepts();
        }catch (InterruptedException e)
        {
            logger.error("unable to send accept request",e);
        }
    }

    /**
     * Broadcast the prepare request to the servers
     * @param prepareRequest
     * @param acceptors
     */
    protected void sendPrepareRequest(PrepareRequest prepareRequest, List<Server> acceptors)
    {
        try
        {
            logger.debug("Sending prepare request");
            sendRequest(prepareRequest, acceptors);
            // Increment the number of promises for ourself
            this.serverThread.incrementNumPromises();
        }catch (InterruptedException e)
        {
            logger.error("unable to send prepare request",e);
        }
    }


    /**
     * Send the prepare request to the acceptors
     * @param acceptors
     * @return
     */
    public boolean propose(List<Server> acceptors)
    {
        int id = this.serverThread.getPaxosId().get();
        String value =  this.serverThread.getPaxosValue();
        logger.debug("Proposing value " + value);
        // send the prepare request to all peers
        PrepareRequest prepareRequest = new PrepareRequest();
        prepareRequest.setId(id);
        prepareRequest.setValue(value);
        prepareRequest.setSenderID(this.serverThread.getHostID().get());
        logger.debug("Sending prepare request to acceptors");
        sendPrepareRequest(prepareRequest,acceptors);
        return true;
    }
}
