package distributed.server.paxos.requests;

import distributed.utils.Command;

public class AcceptRequest extends  Request
{
    @Override
    public String toString()
    {
        return Command.ACCEPT_REQUEST + " " + this.getId() + " " + this.getValue() + "\n";
    }


}
