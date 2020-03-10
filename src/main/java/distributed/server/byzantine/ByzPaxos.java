package distributed.server.byzantine;

import distributed.server.byzantine.propose.ByzProposer;
import distributed.server.paxos.Paxos;
import distributed.server.paxos.propose.Proposer;
import distributed.server.pojos.Server;
import distributed.utils.Command;
import org.apache.log4j.Logger;

import java.util.List;

public class ByzPaxos extends Paxos
{
    private static Logger logger = Logger.getLogger(ByzPaxos.class);

    @Override
    public String proposeValue(String value, List<Server> servers)
    {
        // Update paxosValue
        this.serverThread.setPaxosValue(value);
    
        // Increment the paxos id
        int id =  this.serverThread.getPaxosId().incrementAndGet();

        // Increment the paxos Id
        logger.debug("Proposing value " + value + " with id " + id);

        // Servers list doesn't include "this" server
        int numServers = servers.size() + 1;

        // Phase 1 of Paxos: Propose the value
        ByzProposer proposer = new ByzProposer();
        proposer.setServerThread(this.serverThread);

        proposer.propose(servers);

        // Wait till we get enough promises to move onto phase 2
        try
        {
            this.serverThread.getThreadLock().lock();
            while(this.serverThread.getWeightedPromises().get() <= 3.0/4 && this.serverThread.getWeightPromisesRejected().get() < 1.0/4)
            {
                logger.debug("Waiting for enough promises before moving onto phase 2");
                phase1Condition.await();
            }

        } catch (InterruptedException e)
        {
            logger.error("Unable to await for promises",e);
        }finally
        {
            this.serverThread.getThreadLock().unlock();
        }

        if(this.serverThread.getWeightPromisesRejected().get() >= 1.0/4)
        {
            return "Proposal failed";
        }
        logger.debug("Sending safe request");

        // Let the acceptors know the values are safe
        proposer.safe(servers);

        logger.debug("Starting phase 2");
        // Phase 2 of Paxos: Accept the value
        proposer.accept(servers);
        logger.debug("Waiting for value agreement");
        try
        {
            this.serverThread.getThreadLock().lock();
            while(this.serverThread.getWeightedAccepts().get() <= 3.0/4 && this.serverThread.getWeightAcceptsRejected().get() < 1.0/4)
            {
                logger.debug("Waiting for enough accepts before agreeing to value");

                phase2Condition.await();
            }

        } catch (InterruptedException e)
        {
            logger.error("Unable to await for accepts",e);
        }finally
        {
            this.serverThread.getThreadLock().unlock();

        }
        if(this.serverThread.getWeightAcceptsRejected().get() >= 1.0/4)
        {
            return "Proposal failed";
        }
        logger.debug("Agreed to value");
        // The value is agreed to
        return Command.AGREE.getCommand() + " " + value;

    }

}
