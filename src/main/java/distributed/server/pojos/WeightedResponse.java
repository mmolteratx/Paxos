package distributed.server.pojos;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class WeightedResponse
{
    private String response;
    private Float weight;
}
