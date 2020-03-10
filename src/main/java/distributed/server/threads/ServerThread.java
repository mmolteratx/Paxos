package distributed.server.threads;

import distributed.server.paxos.Paxos;
import distributed.server.pojos.ProposedValue;
import distributed.server.pojos.SafeValue;
import distributed.server.pojos.Server;
import distributed.server.pojos.AtomicFloat;
import distributed.utils.Utils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ServerThread implements Runnable
{
    private static Logger logger = Logger.getLogger(ServerThread.class);

    @Getter @Setter(AccessLevel.PUBLIC)
    private Integer serverId;
    @Getter @Setter(AccessLevel.PUBLIC)
    private Integer port;
    @Getter @Setter(AccessLevel.PUBLIC)
    private String ipAddress;
    @Getter @Setter(AccessLevel.PUBLIC)
    private List<Server> peers;

    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicInteger numPromises;
    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicInteger numAccepts;
    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicInteger numPromisesRejected;
    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicInteger numAcceptsRejected;

    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicFloat weightedPromises;
    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicFloat weightedAccepts;
    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicFloat weightPromisesRejected;
    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicFloat weightAcceptsRejected;

    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicFloat ownWeight;
    @Getter @Setter(AccessLevel.PRIVATE)
    private AtomicInteger hostID;

    // The Paxos Id
    @Getter @Setter(AccessLevel.PUBLIC)
    private AtomicInteger paxosId;
    // The Paxos value
    @Getter @Setter(AccessLevel.PUBLIC)
    private String paxosValue;

    // Proposed and Safe Values
    private Map<SafeValue,AtomicBoolean> safeValues;
    private Map<ProposedValue,AtomicBoolean> proposedValues;

    @Getter @Setter(AccessLevel.PUBLIC)
    private Paxos paxos;

    @Getter (AccessLevel.PUBLIC)
    private final Lock threadLock;
    // Conditionals to wait for promises and accepts from a Byzquorum of weights
    private final Condition waitForPromises;
    private final Condition waitForSafe;
    private final Condition waitForAccepts;

    @Getter
    private AtomicBoolean safeBroadcastDone = new AtomicBoolean(false);

    public void init()
    {
        numPromises = new AtomicInteger(0);
        numAccepts = new AtomicInteger(0);
        numAcceptsRejected = new AtomicInteger(0);
        numPromisesRejected = new AtomicInteger(0);
        safeValues = new HashMap<>();
        proposedValues = new HashMap<>();
        weightedPromises = new AtomicFloat();
        weightedAccepts = new AtomicFloat();
        weightAcceptsRejected = new AtomicFloat();
        weightPromisesRejected = new AtomicFloat();
    }


    public ServerThread(Float weight, int id)
    {
        this.ownWeight = new AtomicFloat(weight);
        this.hostID = new AtomicInteger(id);
        
        paxosId = new AtomicInteger(0);
        threadLock = new ReentrantLock();
        waitForPromises = threadLock.newCondition();
        waitForAccepts = threadLock.newCondition();
        waitForSafe = threadLock.newCondition();
    }

    public void incrementNumPromises()
    {
        int numPromises = this.numPromises.incrementAndGet();
        int numServers = this.peers.size() + 1;
        if(numPromises >= (numServers/2) + 1)
        {
            notifyPromises();
        }
    }

    public void incrementNumAccepts()
    {
        int numAccepts = this.numAccepts.incrementAndGet();
        int numServers = this.peers.size() + 1;
        if(numAccepts >= (numServers/2) + 1)
        {
            notifyAccepts();
        }
    }

    public void incrementNumPromisesRejected()
    {
        int numPromisesRejected = this.numPromisesRejected.incrementAndGet();
        int numServers = this.peers.size() + 1;
        if(numPromisesRejected > (numServers/2) + 1)
        {
            logger.debug("Majority of servers rejected the prepare");
            // majority of peers have rejected, stop waiting for phase2
            notifyPromises();
        }

    }


    public void incrementNumAcceptsRejected()
    {
        int numAcceptsRejected = this.numAcceptsRejected.incrementAndGet();
        int numServers = this.peers.size() + 1;
        if(numAcceptsRejected > (numServers/2) + 1)
        {
            logger.debug("Majority of servers rejected the accept");
            notifyAccepts();

        }

    }

    public void updateWeightPromisesRejected(float responderWeight)
    {
        this.weightPromisesRejected.set(this.weightPromisesRejected.get() + responderWeight);
        float weightPromisesRejected = this.weightPromisesRejected.get();
        if(weightPromisesRejected > 1.0/6)
        {
            logger.debug("No Byzquorum possible.");
            // enough weights of peers have rejected, stop waiting for promise (phase 1)
            notifyPromises();
        }

    }


    public void updateWeightAcceptsRejected(float responderWeight)
    {
        this.weightAcceptsRejected.set(this.weightAcceptsRejected.get() + responderWeight);
        float weightAcceptsRejected = this.weightAcceptsRejected.get();
        if(weightAcceptsRejected > 1.0/6)
        {
            logger.debug("No Byzquorum possible.");
            notifyAccepts();

        }

    }

    private void notifyAccepts()
    {
        // enough weights of peers have rejected, stop waiting for agreement (phase 2)
        threadLock.lock();
        synchronized (waitForAccepts)
        {
            waitForAccepts.notifyAll();
        }
        threadLock.unlock();

    }

    private void notifyPromises()
    {
        threadLock.lock();
        synchronized (waitForPromises)
        {
            waitForPromises.notifyAll();

        }
        threadLock.unlock();
    }

    public void updatePromisedWeight(float responderWeight)
    {
        this.weightedPromises.set((float)(this.weightedPromises.get() + responderWeight));
        float weightedPromises = this.weightedPromises.get();
        if(weightedPromises > 5.0/6)
        {
            notifyPromises();
        }
    }

    public void updateAcceptedWeight(float responderWeight)
    {
        this.weightedAccepts.set((float)(this.weightedAccepts.get() + responderWeight));
        float weightedAccepts = this.weightedAccepts.get();
        if(weightedAccepts > 5.0/6)
        {
            notifyAccepts();
        }
    }


    private AtomicBoolean isRunning = new AtomicBoolean(false);

    public void setPaxosValue(String value)
    {
        threadLock.lock();
        paxosValue = String.copyValueOf(value.toCharArray());
        threadLock.unlock();
    }

    public AtomicBoolean isValueSafe(SafeValue value)
    {
        return safeValues.get(value);
    }

    public AtomicBoolean isValueProposed(ProposedValue value)
    {
        return proposedValues.get(value);
    }


    public void addSafeValue(SafeValue value, boolean isSafe)
    {
        threadLock.lock();
        safeValues.put(value, new AtomicBoolean(isSafe));
        threadLock.unlock();
    }

    public void addProposedValue(ProposedValue value)
    {
        threadLock.lock();
        proposedValues.put(value,new AtomicBoolean(true));
        threadLock.unlock();
    }

    public String getPaxosValue()
    {
        if (paxosValue == null)
        {
            return null;
        }
        String result;
        threadLock.lock();
        result = String.copyValueOf(paxosValue.toCharArray());
        threadLock.unlock();
        return result;
    }
    

    /**
     * Listen for messages from the peers
     */
    public void run()
    {
        this.isRunning.getAndSet(true);
        logger.debug("Starting server thread with ip: " + this.ipAddress + " port: " + this.port);
        ServerSocket tcpServerSocket = null;
        init();
        try
        {
            tcpServerSocket = new ServerSocket(this.port);
            while(this.isRunning.get() == true)
            {
                Socket socket = null;
                String senderIP = null;
                Integer senderPort = null;
                try
                {
                    // Open a new socket with clients
                    socket = tcpServerSocket.accept();
                    // Get sender info to pass to MessageThread
                    senderIP = socket.getInetAddress().getHostAddress();
                    senderPort = socket.getPort();
                    logger.debug("Accepted client connection from " + senderIP + ":" + senderPort);
                }catch (IOException e)
                {
                    logger.error("Unable to accept client socker",e);
                }
                if(socket != null)
                {
                    Server sender = Utils.getSender(senderIP, senderPort, peers);
                    // Spawn off a new thread to process messages from this client
                    MessageThread clientThread = new MessageThread(sender);
                    clientThread.setSocket(socket);
                    clientThread.setPeers(peers);
                    clientThread.setPhase1Condition(waitForPromises);
                    clientThread.setPhase1cCondition(waitForSafe);
                    clientThread.setPhase2Condition(waitForAccepts);
                    clientThread.setServerThread(this);
                    new Thread(clientThread).start();
                }
            }

        }catch (Exception e)
        {
            logger.error("Unable to listen for clients",e);
        }finally
        {
            if(tcpServerSocket != null)
            {
                try
                {
                    tcpServerSocket.close();
                }catch (Exception e)
                {
                    logger.error("Unable to close server socket");
                }
            }
        }

        logger.debug("Stopping server thread");

    }

}
