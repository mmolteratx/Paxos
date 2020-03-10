package distributed.server.paxos.learn;

import distributed.server.paxos.requests.Request;
import distributed.server.pojos.Server;
import lombok.Data;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

@Data
public class Learner
{
    private static Logger logger = Logger.getLogger(Learner.class);
    
    private String value;


    public void sendLearnToPeer(Request request, Server peer)
    {
        String command = request.toString();
        // Send the command over TCP
        Socket tcpSocket = null;

        try
        {
            // Get socket
            tcpSocket = new Socket(peer.getIpAddress(), peer.getPort());
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);
            BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream()));
            // Send learned value message
            outputWriter.write("LEARN " + value + "\n");
            outputWriter.flush();

        } catch (Exception e)
        {
            logger.error("Unable to send msg to " + peer.toString(),e);
        } finally
        {
            if (tcpSocket != null)
            {
                try
                {
                    tcpSocket.close();
                } catch (Exception e)
                {
                    logger.error("Unable to close socket",e);
                }
            }

        }


    }
  
}
