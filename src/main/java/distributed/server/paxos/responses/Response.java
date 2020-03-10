package distributed.server.paxos.responses;

import lombok.Data;

@Data
public abstract class Response
{
    int id;
    String value;
    boolean responseAccepted;

    public Response(int id, String value,boolean responseAccepted)
    {
        this.id = id;
        this.value = value;
        this.responseAccepted = responseAccepted;
    }

}
