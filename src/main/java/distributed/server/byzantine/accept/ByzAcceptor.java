package distributed.server.byzantine.accept;

import distributed.server.paxos.accept.Acceptor;
import distributed.server.pojos.ProposedValue;
import distributed.server.pojos.SafeValue;
import distributed.server.pojos.Server;
import distributed.server.pojos.WeightedResponse;
import distributed.server.threads.BroadcastThread;
import distributed.server.threads.WeightedBroadcastThread;
import distributed.server.threads.WeightedRequestThread;
import distributed.utils.Command;
import distributed.utils.Utils;
import lombok.AccessLevel;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;

/**
 * Byzantine acceptor
 */
public class ByzAcceptor extends Acceptor
{
    @Setter(AccessLevel.PUBLIC)
    protected List<Server> acceptors;

    @Setter(AccessLevel.PUBLIC)
    private Condition waitForSafe;

    private static Logger logger = Logger.getLogger(ByzAcceptor.class);
    private static int NUM_BROADCAST_THREADS = 5;

    public String receiveSafeRequest(String[] tokens)
    {
        if (tokens.length < 3)
        {
            return null;
        }
        int id = Integer.parseInt(tokens[1]);
        String value = tokens[2];
        int senderID = Integer.parseInt(tokens[3]);
        logger.debug("Received safe request with id: " + id + " value: " + value + " from serverID " + senderID);
        SafeValue safeValue = new SafeValue();
        safeValue.setId(id);
        safeValue.setValue(value);
        // Add the value, mark it as not safe for not
        logger.debug("Adding value to map with isSafe=false");
        this.serverThread.addSafeValue(safeValue,false);


        // Broadcast the safe to the rest of the acceptors
        Runnable broadcastSafeRunnable = () ->
        {
            logger.debug("Broadcasting safe request to other acceptors.");
            boolean isValueSafe = broadcastSafeRequest(id,value,this.acceptors, senderID);
            logger.debug(("Is value safe?: " + isValueSafe));

            // Mark the value as safe or not depending
            this.serverThread.addSafeValue(safeValue,isValueSafe);
            // Inform others the safe broadcast is done
            this.serverThread.getSafeBroadcastDone().set(true);
            logger.debug(("Getting lock"));
            this.serverThread.getThreadLock().lock();
            synchronized (waitForSafe)
            {
                logger.debug("Notifiying everyone that wait for safe is done");
                waitForSafe.signalAll();
            }
            this.serverThread.getThreadLock().unlock();
        };
        new Thread(broadcastSafeRunnable).start();
        return null;
    }

    /**
     * TODO: Match the signatures so this method is overriden
     * @param tokens
     * @param sender
     * @return
     */
    public String receivePromiseRequest(String[] tokens, Server sender)
    {
        logger.debug("Received promise request");
        int id = Integer.parseInt(tokens[1]);
        if(id > this.serverThread.getPaxosId().get())
        {
            this.serverThread.getPaxosId().set(id);
        }
        this.serverThread.updatePromisedWeight(sender.getWeight());
        return "Received promise request";
    }

    @Override
    public String receiveAcceptRequest(String[] tokens)
    {
        int id = Integer.parseInt(tokens[1]);
        String value = tokens[2];
        logger.debug("Received accept request with id: " + id + " value: " + value);
        // Wait until the safe broadcast has finished before starting here
        try
        {
            this.serverThread.getThreadLock().lock();
            while(this.serverThread.getSafeBroadcastDone().get() == false)
            {
                logger.debug("Waiting for safe broadcast to finish before proceeding");
                waitForSafe.await();
            }
        } catch (InterruptedException e) {
            logger.error("Unable to wait for safe broadcast to finish",e);
        }finally
        {
            this.serverThread.getSafeBroadcastDone().set(false);

            this.serverThread.getThreadLock().unlock();
        }

        logger.debug("Proceeding");

        SafeValue safeValue = new SafeValue();
        safeValue.setId(id);
        safeValue.setValue(value);
        // check that the value is safe
        logger.debug("Checking that the value is safe");
        AtomicBoolean isValueSafe = this.serverThread.isValueSafe(safeValue);
        if (isValueSafe == null || isValueSafe.get() == false)
        {
            logger.debug("Value is not safe");
            // Value is not safe. reject the request
            return Command.REJECT_ACCEPT.getCommand() + " " + id + " " + value;
        }
        logger.debug("Value is safe. Continuing with the paxos impl");
        // Value is safe, follow the paxos algo for receiving accept request
        return super.receiveAcceptRequest(tokens);
    }

    /**
     * TODO: Check if this is the correct input to this method
     * @param sender
     * @return
     */
    public synchronized String receiveAcceptResponse(Server sender)
    {
        logger.debug("Received accept reponser from server " + sender );
        this.serverThread.getWeightedAccepts().set(this.serverThread.getWeightedAccepts().get() + sender.getWeight());
        if(this.serverThread.getWeightedAccepts().get() > 3.0/4)
        {
            this.serverThread.getThreadLock().lock();
            logger.debug("We've received enough accepts. can agree on a value");
            // We've received enough accepts. can agree on a value
            this.phase2Condition.signalAll();
            this.serverThread.getThreadLock().unlock();
        }
        return "Agreed to value";
    }


    @Override
    public String receivePrepareRequest(String[] tokens)
    {
        if (tokens.length < 3)
        {
            return Command.REJECT_PREPARE.getCommand() + " " + this.serverThread.getPaxosId().get() + " " + this.serverThread.getPaxosValue() + "\n";
        }
        int id = Integer.parseInt(tokens[1]);
        String value = tokens[2];
        int senderID = Integer.parseInt(tokens[3]);
        logger.debug("Receiving prepare request with id " + id + " value " + value);
        ProposedValue proposedValue = new ProposedValue();
        proposedValue.setId(id);
        proposedValue.setValue(value);
        this.serverThread.addProposedValue(proposedValue);

        // Compare the id to our current id
        if(id > this.serverThread.getPaxosId().get())
        {
            String valuePreviouslyPromised = updateValues(id,value);


            logger.debug("Broadcasting prepare request");
            // Broadcast the prepare request to the other acceptors
            boolean broadcastSuccessful = broadcastPrepareRequest(id,value,this.acceptors, senderID);
            if(broadcastSuccessful == false)
            {
                logger.debug("Rejecting prepare request");
                return Command.REJECT_PREPARE.getCommand() + " " + this.serverThread.getPaxosId().get() + " " + this.serverThread.getPaxosValue() + "\n";
            }
            else
            {
                return Command.PROMISE.getCommand() + " " + this.serverThread.getPaxosId().get() + " " + valuePreviouslyPromised + "\n";
            }

        }
        logger.debug("Rejecting prepare request with id " + id + " value " + value);
        // REJECT the request and send the paxos id and value
        return Command.REJECT_PREPARE.getCommand() + " " + this.serverThread.getPaxosId().get() + " " + this.serverThread.getPaxosValue() + "\n";

    }

    private boolean broadcastPrepareRequest(int id, String value, List<Server> acceptors, int senderID)
    {
        // Broadcast the request we received from a proposer to all peers
        String cmd = Command.PREPARE_BROADCAST.getCommand() + " " + id + " " + value + "\n";
        try
        {
            return broadcastCommand(cmd,acceptors,senderID);
        }catch (Exception e)
        {
            logger.debug("Unable to broadcast prepare request",e);
        }
        return false;
    }

    private boolean broadcastSafeRequest(int id, String value, List<Server> acceptors, int senderID)
    {
        // Broadcast the request we received from a proposer to all peers
        String cmd = Command.SAFE_BROADCAST.getCommand() + " " + id + " " + value + "\n";
        try
        {
            return broadcastCommand(cmd, acceptors, senderID);
        }catch (Exception e)
        {
            logger.debug("Unable to broadcast safe request",e);
        }
        return false;
    }

    private synchronized boolean broadcastCommand(String cmd, List<Server> acceptors, int senderID) throws InterruptedException
    {
        return broadcastCommand(cmd,acceptors,senderID,0);

    }


    private synchronized boolean broadcastCommand(String cmd, List<Server> acceptors, int senderID, int numFaulty) throws InterruptedException
    {
        // Execute broadcast in executor service
        ExecutorService executor = Executors.newFixedThreadPool(NUM_BROADCAST_THREADS);
        List<Callable<WeightedResponse>> workers = new ArrayList<>();


        logger.debug("Broadcasting command " + cmd + "  to " + acceptors.toString());
        double acceptedWeight = 0.0;
        double rejectedWeight = 0.0;
        // Add our own weight and the weight of the sender
        acceptedWeight += this.serverThread.getOwnWeight().doubleValue();
        double quorumWeight = Utils.getQuorumWeight(.25);
        logger.debug("Quorum weight : " + quorumWeight);
        for(Server acceptor: acceptors)
        {

            // Don't broadcast the message to the proposer
            if (!acceptor.getServerId().equals(senderID))
            {
                Callable<WeightedResponse> worker = new WeightedBroadcastThread(acceptor,cmd,true,acceptor.getWeight());
                workers.add(worker);
            }else
            {
                // Add the proposer's accepted weight
                acceptedWeight += acceptor.getWeight().doubleValue();
            }
        }

        List<Future<WeightedResponse>> responses = executor.invokeAll(workers);
        for(Future<WeightedResponse> futureResponse : responses)
        {
            try
            {
                String response = futureResponse.get().getResponse();
                Float weight = futureResponse.get().getWeight();
                if (Command.SAFE_BROADCAST_ACCEPT.getCommand().equals(response) || Command.PREPARE_BROADCAST_ACCEPT.getCommand().equals(response)) {
                    acceptedWeight += weight.doubleValue();
                    logger.debug("Received accept with weight " + weight + ". Accepted weight: " + acceptedWeight);
                } else {
                    rejectedWeight += weight.doubleValue();
                    logger.debug("Received reject with weight " + weight + ". Rejected weight " + rejectedWeight);
                }
                if(acceptedWeight >= quorumWeight)
                {
                    logger.debug("Accepts reached quorum. Accepting");
                    return true;
                }
                if(rejectedWeight > (1.0 - quorumWeight))
                {
                    logger.debug("Too many rejects. Quorum not possible. Bailing");
                    return false;
                }

            }catch (Exception e)
            {
                logger.debug("Unable to process broadcast response from acceptor",e);
            }
        }

        return false;

    }

    public String receiveSafeBroadcast(String[] tokens)
    {
        int id = Integer.parseInt(tokens[1]);
        String value = tokens[2];
        logger.debug("Received safe broadcast with id: "  + id + " value " + value);

        SafeValue safeValue = new SafeValue();
        safeValue.setId(id);
        safeValue.setValue(value);

        // Check that we've receive this request previously
        if(this.serverThread.isValueSafe(safeValue) == null)
        {
            logger.debug("We have not received this safe value. Rejecting");
            // The safe request that was broadcast doesn't match what we've received
            return Command.SAFE_BROADCAST_REJECT.getCommand();
        }

        logger.debug("Accepting safe broadcast");
        return Command.SAFE_BROADCAST_ACCEPT.getCommand();
    }

    public String receivePrepareBroadcast(String[] tokens)
    {
        int id = Integer.parseInt(tokens[1]);
        String value = tokens[2];
        logger.debug("Received prepare broadcast with id: "  + id + " value " + value);
        ProposedValue proposedValue = new ProposedValue();
        proposedValue.setId(id);
        proposedValue.setValue(value);
        // Check the id of the broadcast matches what we received
        if(this.serverThread.isValueProposed(proposedValue) == null)
        {
            logger.debug("We have not received this proposed value. Rejecting");
            // The safe request that was broadcast doesn't match what we've received
            return Command.PREPARE_BROADCAST_REJECT.getCommand();
        }

        logger.debug("Accepting prepare broadcast");
        return Command.PREPARE_BROADCAST_ACCEPT.getCommand();
    }
}
