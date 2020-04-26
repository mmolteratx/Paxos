package distributed.server.pojos;

import lombok.AllArgsConstructor;
import java.util.Objects;

@AllArgsConstructor
public class ProposedValue extends Value
{
    @Override
    public int hashCode()
    {
        return Objects.hash(id,value);
    }
}
