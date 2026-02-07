package pe.com.comsatel.clocator.microservice.posiciones.infrastructure.adapter.out.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;

import org.apache.tomcat.util.bcel.Const;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import pe.com.comsatel.clocator.microservice.posiciones.application.ports.out.ProtocolTrackSolidClientOutPort;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.tracksolid.ReturnTagMsgTrackSolid;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.type.ProtocoloTagPb705Type;
import pe.com.comsatel.clocator.microservice.posiciones.shared.constans.Constants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

@Component
public class ProtocolTrackSolidClientAdapter implements ProtocolTrackSolidClientOutPort {
    private static final Logger LOG = LoggerFactory.getLogger(ProtocolTrackSolidClientAdapter.class);
   

    @CircuitBreaker(name = "serviceSendProtocolTrackSolid", fallbackMethod = "fallbackServiceSendProtocolTrackSolid")
    @Retry(name = "serviceSendProtocolTrackSolid")
    @Override
    public void sendProtocolTrackSolid(String imei, List<ReturnTagMsgTrackSolid> positions, Map<String, String> servidor) throws IOException {
        
        String host = servidor.get(Constants.POSITION_SERVER_HOST);
        String port = servidor.get(Constants.POSITION_SERVER_PORT);

        ProtocoloTagPb705Type protocol;

        Socket socket;
        DataOutputStream outputStream;

        socket = new Socket(host, Integer.parseInt(port));
        socket.setSoTimeout(60000);
        LOG.debug(String.format("Socket: %s:%s", host, port));
        outputStream = new DataOutputStream(socket.getOutputStream());

        for (ReturnTagMsgTrackSolid msg : positions) {
            protocol = new ProtocoloTagPb705Type();
            msg.setImei(imei);
            msg.setName("");
            protocol.Parse(msg);

            LOG.debug("Trama JC450: " + protocol.getTrama()); //CP04
            outputStream.write(protocol.getTrama().getBytes());
            outputStream.flush();
        }

        outputStream.close();
        socket.close();
    }

     public void fallbackServiceSendProtocolTrackSolid(Exception e) throws Exception {
         LOG.info("Error fallbackServiceSendProtocolTrackSolid - Message: {}", e.getMessage());
         throw e;
     }
}
