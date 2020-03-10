package distributed.server.byzantine.requests;

import distributed.server.paxos.requests.Request;
import distributed.utils.Command;

public class SafeRequest extends Request
{
    @Override
    public String toString()
    {
        return Command.SAFE_REQUEST + " " + this.getId() + " " + this.getValue() + " " + this.getSenderID() + "\n";
    }

}
