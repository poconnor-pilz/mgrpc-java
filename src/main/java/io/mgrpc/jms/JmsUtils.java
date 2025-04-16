package io.mgrpc.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.lang.invoke.MethodHandles;

public class JmsUtils {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static byte[] byteArrayFromMessage(Session session, Message message) throws Exception{
        BytesMessage bytesMessage = (BytesMessage) message;
        byte[] buffer = new byte[(int) bytesMessage.getBodyLength()];
        bytesMessage.readBytes(buffer);
        return buffer;
    }

    public static BytesMessage messageFromByteArray(Session session, byte[] buffer) throws JMSException {
        final BytesMessage bytesMessage = session.createBytesMessage();
        bytesMessage.writeBytes(buffer);
        return bytesMessage;
    }

}
