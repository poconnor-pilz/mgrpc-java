package io.mgrpc.errors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class CloseableSocketFactory extends SocketFactory {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public List<Socket> sockets = new ArrayList<>();

    private boolean enableSocketCreation = true;

    public CloseableSocketFactory() throws Exception {
        super();
    }

    public Socket createSocket() {
        return checkEnabled(new Socket());
    }

    public Socket createSocket(String host, int port)
            throws IOException, UnknownHostException
    {
        return checkEnabled(new Socket(host, port));
    }

    public Socket createSocket(InetAddress address, int port)
            throws IOException
    {
        return checkEnabled(new Socket(address, port));
    }

    public Socket createSocket(String host, int port,
                               InetAddress clientAddress, int clientPort)
            throws IOException, UnknownHostException
    {
        return checkEnabled(new Socket(host, port, clientAddress, clientPort));
    }

    public Socket createSocket(InetAddress address, int port,
                               InetAddress clientAddress, int clientPort)
            throws IOException
    {
        return checkEnabled(new Socket(address, port, clientAddress, clientPort));
    }

    public Socket checkEnabled(Socket socket){
        if(!enableSocketCreation){
            try {
                socket.close();
            } catch (IOException e) {
                log.error("Error closing socket", e);
                throw new RuntimeException(e);
            }
        }

        sockets.add(socket);
        return socket;
    }


    public void disableAndCloseAll() throws IOException {
        enableSocketCreation(false);
        closeAllSockets();
    }

    /**
     * Cause creation of new sockets to succeed/fail
     * @param enable
     */
    public void enableSocketCreation(boolean enable){
        this.enableSocketCreation = enable;
    }


    /**
     * Close all sockets created by this factory and remove them from list.
     * Any thread currently blocked in an I/O operation upon each socket
     * will throw a {@link SocketException}.
     */
    public void closeAllSockets() throws IOException{
        for(Socket socket: sockets){
            socket.close();
        }
    }
}