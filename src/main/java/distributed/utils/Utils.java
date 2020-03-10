package distributed.utils;

import distributed.server.pojos.Server;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Utils
{
    // Set the timeout when sending tcp messages
    private static int TIMEOUT_MILLIS = 3000;

    private static Logger logger = Logger.getLogger(Utils.class);

    public static Server getSender(String ipAddr, Integer port, List<Server> peers)
    {
        logger.debug(ipAddr + " " + port + " " + peers.toString());
        for (Server server:peers)
        {
            if(ipAddr.equals(server.getIpAddress()))
            {
                if(port.equals(server.getPort()))
                {
                    return server;
                }
            }
        }
        return null;
    }



    // Return the messages' rejection
    private static String rejectMessage(String cmd)
    {
        if(cmd != null)
        {
            String[] tokens = cmd.split("\\s+");
            if(Command.PREPARE_BROADCAST.getCommand().equals(tokens[0]))
            {
                return Command.PREPARE_BROADCAST_REJECT.getCommand();
            }
            else if(Command.SAFE_BROADCAST.getCommand().equals(tokens[0]))
            {
                return Command.SAFE_BROADCAST_REJECT.getCommand();
            }
            else if(Command.ACCEPT_REQUEST.getCommand().equals(tokens[0]))
            {
                return Command.REJECT_ACCEPT.getCommand();
            }else if(Command.PREPARE_REQUEST.getCommand().equals(tokens[0]))
            {
                return Command.REJECT_PREPARE.getCommand();
            }
        }
        return null;

    }

    public static String sendTcpMessage(Server acceptor,String command, boolean waitForResponse)
    {
        Socket tcpSocket = null;
        String response = "";
        try
        {
            // Get the socket
            tcpSocket = new Socket(acceptor.getIpAddress(), acceptor.getPort());
            tcpSocket.setSoTimeout(TIMEOUT_MILLIS);
            PrintWriter outputWriter = new PrintWriter(tcpSocket.getOutputStream(), true);
            BufferedReader inputReader = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream()));
            // Write the message
            outputWriter.write(command);
            outputWriter.flush();
            while(waitForResponse)
            {
                String input = inputReader.readLine();
                if (input == null)
                {
                    break;
                }
                response += input;
            }

        } catch (Exception e)
        {
            logger.debug("Unable to send msg " + command + " to " + acceptor.toString() + ". Assuming acceptor rejected");
            response = rejectMessage(command);
        }finally
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
        return response;
    }

    public static int getQuorumSize(int numServers,int numFaulty)
    {
        return numServers/2 + numFaulty + 1;
    }

    public static double getQuorumWeight(double faultyWeight)
    {
        return 1.0 - faultyWeight;
    }

    public static int getAnchorSize(List<Server> servers, double p)
    {
        double weight = 0;
        int anchorSize = 0;
        for (Server server: servers)
        {
            if(weight > p)
            {
                break;
            }
            weight += server.getWeight();
            anchorSize++;
        }
        return anchorSize;
    }

    // Load the hosts from the yaml file
    public static List<Server> getHosts(String path)
    {
        logger.debug("Getting hosts from hosts.yaml");
        InputStream inputStream;

        try
        {
            inputStream = new FileInputStream(path);
        }catch (FileNotFoundException e)
        {
            logger.error("Unable to find hosts file",e);
            return null;
        }

        if(inputStream == null)
        {
            logger.error("Unable to load from hosts.yaml");
            return null;
        }
        Yaml yaml = new Yaml(new Constructor(Server.class));
        List<Server> hosts = new ArrayList<Server>();
        for (Object o : yaml.loadAll(inputStream))
        {
            hosts.add( (Server) o);
        }
        try
        {
            inputStream.close();
        }catch (IOException e)
        {

            logger.error("Unable to close input stream",e);
        }
        return hosts;
    }
}
