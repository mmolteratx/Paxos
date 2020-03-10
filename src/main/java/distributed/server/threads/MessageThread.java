package distributed.server.threads;

import distributed.server.byzantine.ByzPaxos;
import distributed.server.byzantine.accept.ByzAcceptor;
import distributed.server.paxos.Paxos;
import distributed.server.paxos.accept.Acceptor;
import distributed.server.pojos.Server;
import distributed.utils.Command;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class MessageThread implements Runnable
{
    private static Logger logger = Logger.getLogger(MessageThread.class);

    @Setter(AccessLevel.PUBLIC)
    private Socket socket;

    @Setter(AccessLevel.PUBLIC)
    private ServerThread serverThread;

    @Setter(AccessLevel.PUBLIC)
    private List<Server> peers;

    @Setter(AccessLevel.PUBLIC)
    private Condition phase1Condition;

    @Setter(AccessLevel.PUBLIC)
    private Condition phase2Condition;

    @Setter(AccessLevel.PUBLIC)
    private Condition phase1cCondition;

    @Getter
    private Server sender;

    public MessageThread(Server sender) {
        this.sender = sender;
    }

   // Start the paxos algorithm to propose the value
    private String proposeValue(String value)
    {
        logger.debug("Proposing value using paxos: " + value);

        ByzPaxos paxos = new ByzPaxos();
        paxos.setValue(value);
        paxos.setServers(this.peers);
        paxos.setPhase1Condition(this.phase1Condition);
        paxos.setPhase2Condition(this.phase2Condition);
        paxos.setServerThread(this.serverThread);

        // Before starting, check if there is already a paxos proposal running
        // If so, stop it and reset for the new proposal

        if(this.serverThread.getPaxos() != null)
        {
            this.serverThread.init();
        }
        this.serverThread.setPaxos(paxos);
        return paxos.proposeValue();
    }


    public String receivePrepareRequest(String[] tokens)
    {
        ByzAcceptor acceptor = new ByzAcceptor();
        acceptor.setServerThread(this.serverThread);
        List<Server> acceptors = new ArrayList<>(this.peers);
        // Remove the sender from the list of acceptors to broadcast to
        acceptors.remove(sender);
        acceptor.setAcceptors(acceptors);
        acceptor.setWaitForSafe(this.phase1cCondition);
        return acceptor.receivePrepareRequest(tokens);
    }

    public synchronized String receivePromiseRequest(String[] tokens, Server sender)
    {
        ByzAcceptor acceptor = new ByzAcceptor();
        acceptor.setServerThread(this.serverThread);
        acceptor.setWaitForSafe(this.phase1cCondition);
        acceptor.setAcceptors(this.peers);
        acceptor.receivePromiseRequest(tokens, sender);
        // Return null. Don't need to write anything back
        return null;
    }


    public String receiveAcceptRequest(String[] tokens)
    {
        ByzAcceptor acceptor = new ByzAcceptor();
        acceptor.setServerThread(this.serverThread);
        acceptor.setWaitForSafe(this.phase1cCondition);
        acceptor.setAcceptors(this.peers);
        return acceptor.receiveAcceptRequest(tokens);
    }

    public synchronized String receiveAcceptResponse(Server sender)
    {
        ByzAcceptor acceptor = new ByzAcceptor();
        acceptor.setServerThread(this.serverThread);
        acceptor.setPhase2Condition(phase2Condition);
        acceptor.setAcceptors(this.peers);
        acceptor.setWaitForSafe(this.phase1cCondition);
        return acceptor.receiveAcceptResponse(sender);
    }

    private String receiveSafeRequest(String[] tokens)
    {
        ByzAcceptor acceptor = new ByzAcceptor();
        acceptor.setServerThread(this.serverThread);
        acceptor.setPhase2Condition(phase2Condition);
        List<Server> acceptors = new ArrayList<>(this.peers);
        // Remove the sender from the list of acceptors to broadcast to
        acceptors.remove(sender);
        acceptor.setAcceptors(acceptors);
        acceptor.setWaitForSafe(this.phase1cCondition);
        return acceptor.receiveSafeRequest(tokens);
    }

    private String receiveSafeBroadcast(String[] tokens)
    {

        ByzAcceptor acceptor = new ByzAcceptor();
        acceptor.setServerThread(this.serverThread);
        acceptor.setPhase2Condition(phase2Condition);
        acceptor.setAcceptors(this.peers);
        acceptor.setWaitForSafe(this.phase1cCondition);
        return acceptor.receiveSafeBroadcast(tokens);

    }

    private String receivePrepareBroadcast(String[] tokens)
    {
        ByzAcceptor acceptor = new ByzAcceptor();
        acceptor.setServerThread(this.serverThread);
        acceptor.setPhase2Condition(phase2Condition);
        acceptor.setAcceptors(this.peers);
        acceptor.setWaitForSafe(this.phase1cCondition);
        return acceptor.receivePrepareBroadcast(tokens);

    }


    private String processMessage(String msg, Server sender)
    {
        String[] tokens = msg.split("\\s+");
        if(Command.PROPOSE.getCommand().equals(tokens[0]))
        {
            // The client wants us to agree on a value. Start the paxos value
            if(tokens.length > 1)
            {
                String value = tokens[1];
                return proposeValue(value);
            }
        }else if(Command.PREPARE_REQUEST.getCommand().equals(tokens[0]))
        {
           // A peer sent a prepare request with a value
            if(tokens.length > 2)
            {
               return receivePrepareRequest(tokens);
            }
        }
        else if (Command.PROMISE.getCommand().equals(tokens[0]))
        {
            // A peer promised to accept our prepare request
            if(tokens.length > 2)
            {
                return receivePromiseRequest(tokens, sender);
            }
        }
        else if(Command.ACCEPT_REQUEST.getCommand().equals(tokens[0]))
        {
            // A peer sent an accept request with a value
            if(tokens.length>2)
            {
                return receiveAcceptRequest(tokens) ;
            }
        }
        else if(Command.ACCEPT.getCommand().equals(sender))
        {
            // A peer sent a response to our accept request
            return receiveAcceptResponse(sender);

        }
        else if(Command.SAFE_REQUEST.getCommand().equals(tokens[0])) {
            // A proposer sent us a safe request
            return receiveSafeRequest(tokens);

        }else if(Command.SAFE_BROADCAST.getCommand().equals(tokens[0]))
        {
            // An acceptor is broadcasting the safe request it received
            return receiveSafeBroadcast(tokens);
        }
        else if(Command.PREPARE_BROADCAST.getCommand().equals(tokens[0]))
        {
            // An acceptor is broadcasting the prepare request it received
            return receivePrepareBroadcast(tokens);
        }
        return "Unable to process msg " + msg;
    }
    
    public void run()
    {
        try
        {
            //We have received a TCP socket from the client.  Receive message and reply.
            BufferedReader inputReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter outputWriter = new PrintWriter(socket.getOutputStream(), true);
            String inputLine = inputReader.readLine();
            if (inputLine != null && inputLine.length() > 0)
            {
                String msg = inputLine;
                logger.debug("Processing message: " + msg);

                String response = processMessage(msg,sender);

                // Increment the logical clock on response
                if(response != null)
                {
                    outputWriter.write(response);
                    outputWriter.flush();
                }
                outputWriter.close();
            }
        }catch (IOException e)
        {

            logger.error("Unable to receive msg from client",e);
        }
        finally
        {
            if(socket != null)
            {
                try
                {
                    socket.close();
                }catch (IOException e)
                {
                    logger.error("Unable to close client socket",e);
                }
            }

        }

    }
}
