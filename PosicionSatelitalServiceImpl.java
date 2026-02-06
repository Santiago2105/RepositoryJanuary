package pe.com.comsatel.clocator.microservice.posiciones.domain.internal;

import java.io.DataOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import pe.com.comsatel.clocator.microservice.posiciones.domain.DomainSatelitalService;
import pe.com.comsatel.clocator.microservice.posiciones.domain.PosicionesRepository;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.PosicionSatelitalResponse;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.satelital.ReturnMessage;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.type.PropiedadAppType;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.type.ProtocoloSVRVOType;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.type.ProtocoloSkywaveVOType;

/**
 *
 * @author ianache
 */

@Service
public class PosicionSatelitalServiceImpl implements DomainSatelitalService {

    private static final Logger LOG = LogManager.getLogger(PosicionSatelitalServiceImpl.class);

    final static String OrbCommHost = "isatdatapro.orbcomm.com";
    final static String OrbCommBasePath = "/GLGW/2/RestMessages.svc/JSON/";

    @Value("${skywave.access.id}")
    public String SKYWAVE_ACCESS_ID;

    @Value("${skywave.access.password}")
    public String SKYWAVE_ACCESS_PASSWORD;

    @Value("${skywave.position.host}")
    public String SKYWAVE_ACCESS_HOST;

    @Value("${skywave.position.port}")
    public String SKYWAVE_ACCESS_PORT;

    @Value("${skywave.position.portSVR}")
    public String SKYWAVE_ACCESS_PORTSVR;

    @Autowired
    @Qualifier("MYSQL")
    private PosicionesRepository domainRepository;

    @Override
    public List<PosicionSatelitalResponse> procesarPosicionesSatelitales() {
        // Logica
        try {
            // Parametros de busqueda
            String start_utc = getOrbCommParam();
            String funcionalidad = "get_return_messages/";

            URIBuilder builder = new URIBuilder();
            builder.setScheme("https")
                    .setHost(OrbCommHost)
                    .setPath(OrbCommBasePath + funcionalidad)
                    .setParameter("access_id", SKYWAVE_ACCESS_ID)
                    .setParameter("password", SKYWAVE_ACCESS_PASSWORD)
                    .setParameter("start_utc", start_utc);
            URI orbcommUri = builder.build();

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(orbcommUri)
                    .header("Accept", "application/json")
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            String responseBody = response.body();

            if (response.statusCode() == 200) {
                if (!validarJson(responseBody)) {
                    responseBody = "Error con la obtencion de las posiciones";
                    return generatePosicionSatelitalResponse(responseBody);
                }
                List<ReturnMessage> lstMessages = new ArrayList<>();

                JsonObject jsonObject = new Gson().fromJson(responseBody, JsonObject.class);
                JsonArray messages = jsonObject.getAsJsonArray("Messages");
                Gson gson = new Gson();

                for (JsonElement message : messages) {
                    try {
                        ReturnMessage returnMessage = gson.fromJson(message, ReturnMessage.class);
                        lstMessages.add(returnMessage);
                    } catch (Exception e) {
                        LOG.error(e.getMessage());
                    }
                }

                enviarMensajesTCP(lstMessages);
                grabarNuevaFecha(lstMessages);

                responseBody = String.format("Se procesaron %s posiciones", messages.size());
            }

            // return domainReportesRepository.queryPernocteVehiculos(fecha_corte,
            // compania);
            return generatePosicionSatelitalResponse(responseBody);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
            return generatePosicionSatelitalResponse(e.getMessage());
        }
    }

    private boolean validarJson(String json) {
        try {

            if (json == null || json.isEmpty())
                return false;

            JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
            if (jsonObject.isJsonNull())
                return false;

            if (jsonObject.get("ErrorID").getAsInt() > 0
                    && (jsonObject.get("ErrorID") != null || !jsonObject.get("ErrorID").isJsonNull())) {
                LOG.error("Error al obtener posicion satelital: ErrorId-" + jsonObject.get("ErrorID").getAsString());
                return false;
            }

            if (jsonObject.get("Messages").isJsonNull())
                return false;

            JsonArray messages = jsonObject.getAsJsonArray("Messages");
            if (!messages.isJsonArray())
                return false;

            if (messages.size() < 1)
                return false;

        } catch (Exception e) {
            LOG.error(e.getMessage());
            return false;
        }

        return true;
    }

    private void grabarNuevaFecha(List<ReturnMessage> mensajes) throws Exception {
        String nuevaFecha = String.valueOf(mensajes.get(mensajes.size() - 1).getMessageUTC());
        LOG.debug("Actualiza SkywaveServerFecha: " + nuevaFecha);
        domainRepository.setPropiedadAppValue("SkywaveServer2Fecha", nuevaFecha);
    }

    private List<PosicionSatelitalResponse> generatePosicionSatelitalResponse(String response) {
        List<PosicionSatelitalResponse> lstResponse = new ArrayList<>();
        lstResponse.add(PosicionSatelitalResponse.builder().resultado(response).build());
        return lstResponse;
    }

    private String getOrbCommParam() throws Exception {
        PropiedadAppType param = domainRepository.getPropiedadAppValue("SkywaveServer2Fecha");
        LOG.debug("Fecha a procesar: " + param.getValor());
        return param.getValor();
    }

    private void enviarMensajesTCP(List<ReturnMessage> mensajes) throws Exception {
        try {
            Socket socket;
            DataOutputStream outputStream;
            Socket socketSVR;
            DataOutputStream outputStreamSVR;

            socket = new Socket(SKYWAVE_ACCESS_HOST, Integer.parseInt(SKYWAVE_ACCESS_PORT));
            socket.setSoTimeout(60000);
            LOG.debug(String.format("socket: %s:%s", SKYWAVE_ACCESS_HOST, SKYWAVE_ACCESS_PORT));
            outputStream = new DataOutputStream(socket.getOutputStream());

            socketSVR = new Socket(SKYWAVE_ACCESS_HOST, Integer.parseInt(SKYWAVE_ACCESS_PORTSVR));
            socketSVR.setSoTimeout(60000);
            LOG.debug(String.format("socket: %s:%s", SKYWAVE_ACCESS_HOST, SKYWAVE_ACCESS_PORTSVR));
            outputStreamSVR = new DataOutputStream(socketSVR.getOutputStream());

            for (ReturnMessage msg : mensajes) {
                int sin = msg.getSin();
                if (sin == 128) {
                    ProtocoloSkywaveVOType protocolo = new ProtocoloSkywaveVOType();
                    protocolo.Parse(msg);
                    LOG.debug("trama Skywave: " + protocolo.getTrama());

                    outputStream.write(protocolo.getTrama().getBytes());
                    outputStream.flush();
                } else if (sin == 129) {
                    ProtocoloSVRVOType protocolo = new ProtocoloSVRVOType();
                    protocolo.Parse(msg);
                    LOG.debug("trama SVR: " + protocolo.getTrama());

                    outputStreamSVR.write(protocolo.getTrama().getBytes());
                    outputStreamSVR.flush();
                }
            }

            outputStream.close();
            socket.close();
            outputStreamSVR.close();
            socketSVR.close();
        } catch (SocketException ex) {
            LOG.error("<<SocketException - " + ex.getMessage());
            throw ex;
        } catch (SocketTimeoutException ex) {
            LOG.error("<<SocketTimeoutException - " + ex.getMessage());
            throw ex;
        }
    }

}
