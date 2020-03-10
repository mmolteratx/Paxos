package distributed.server.paxos;

import distributed.server.paxos.propose.Proposer;
import distributed.server.pojos.Server;
import distributed.server.threads.ServerThread;
import distributed.utils.Command;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

@Data
public class Paxos
{
    private static Logger logger = Logger.getLogger(Paxos.class);

    private String value;
    private List<Server> servers;

    @Setter(AccessLevel.PUBLIC)
    protected Condition phase1Condition;

    @Setter(AccessLevel.PUBLIC)
    protected Condition phase2Condition;

    @Setter(AccessLevel.PUBLIC)
    protected ServerThread serverThread;

    public String proposeValue()
    {
        return proposeValue(this.value,this.servers);
    }

    // Start the paxos algorithm to reserve the value
    public String proposeValue(String value, List<Server> servers)
    {
        // Increment the paxos id
        int id =  this.serverThread.getPaxosId().incrementAndGet();

        // Increment the paxos Id
        logger.debug("Proposing value " + value + " with id " + id);

        // Servers list doesn't include "this" server
        int numServers = servers.size() + 1;

        // Phase 1 of Paxos: Propose the value
        Proposer proposer = new Proposer();
        proposer.setServerThread(this.serverThread);

        proposer.propose(servers);

        // Wait till we get enough promises to move onto phase 2
        try
        {
            this.serverThread.getThreadLock().lock();
            while(this.serverThread.getNumPromises().get() < (numServers / 2  + 1))
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

        if(this.serverThread.getNumPromisesRejected().get() < (numServers / 2  + 1))
        {
            return "Proposal failed";
        }

        logger.debug("Starting phase 2");
        // Phase 2 of Paxos: Accept the value
        proposer.accept(servers);
        logger.debug("Waiting for value agreement");
        try
        {
            this.serverThread.getThreadLock().lock();
            while(this.serverThread.getNumAccepts().get() < (numServers / 2  + 1))
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
        if(this.serverThread.getNumAcceptsRejected().get() < (numServers / 2  + 1))
        {
            return "Proposal failed";
        }
        logger.debug("Agreed to value");
        // The value is agreed to
        return Command.AGREE.getCommand() + " " + value;
    }
}
