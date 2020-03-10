package distributed.server.paxos.responses;

import distributed.utils.Command;

public class AcceptResponse extends Response
{
    public AcceptResponse(int id, String value,boolean accepted)
    {
        super(id,value,accepted);
    }

    @Override
    public String toString()
    {
        return Command.ACCEPT + " " + this.getId() + " " + this.getValue() + "\n";
    }

}
