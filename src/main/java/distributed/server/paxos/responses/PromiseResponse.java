package distributed.server.paxos.responses;

import distributed.utils.Command;

public class PromiseResponse extends Response
{
    public PromiseResponse(int id, String value, boolean accepted)
    {
        super(id,value,accepted);
    }

    @Override
    public String toString()
    {
        return Command.PROMISE + " " + this.getId() + " " + this.getValue() + "\n";
    }
}
