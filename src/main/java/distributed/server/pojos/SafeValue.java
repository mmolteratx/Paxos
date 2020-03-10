package distributed.server.pojos;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@AllArgsConstructor
public class SafeValue extends Value
{

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
        {
            return true;
        }

        /* Check if o is an instance of Complex or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof Value))
        {
            return false;
        }

        Value v = (Value) o;
        if (v != null)
        {
            if (v.getId() == this.getId())
            {
                if (v.getValue() != null && v.getValue().equals(this.getValue()))
                {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id,value);
    }
}
