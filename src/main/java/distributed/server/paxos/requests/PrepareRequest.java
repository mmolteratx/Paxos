package distributed.server.paxos.requests;

import distributed.utils.Command;

public class PrepareRequest extends Request
{
    @Override
    public String toString()
    {
        return Command.PREPARE_REQUEST + " " + this.getId() + " " + this.getValue() + " " + this.getSenderID() + "\n";
    }
}
