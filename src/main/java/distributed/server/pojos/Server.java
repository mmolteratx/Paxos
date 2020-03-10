package distributed.server.pojos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Server
{
    private String ipAddress;
    private Integer port;
    private Integer serverId;
	
    @Getter @Setter(AccessLevel.PUBLIC)
    private Float weight;

    public Server(String ipAddress)
	{
		this.ipAddress = ipAddress;
	}
}
