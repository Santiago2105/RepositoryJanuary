package pe.com.comsatel.clocator.microservice.posiciones.domain.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import pe.com.comsatel.clocator.microservice.posiciones.application.ApplicationException;
import pe.com.comsatel.clocator.microservice.posiciones.application.ports.in.tokentracksolid.PositionTagMessageUseCase;
import pe.com.comsatel.clocator.microservice.posiciones.domain.DomainSatelitalTrackSolidService;
import pe.com.comsatel.clocator.microservice.posiciones.domain.PosicionesRepository;
import pe.com.comsatel.clocator.microservice.posiciones.domain.ProveedoresRepository;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.PosicionSatelitalResponse;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.tracksolid.*;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.type.PropiedadAppType;
import pe.com.comsatel.clocator.microservice.posiciones.domain.model.type.ProtocoloJC450Type;
import pe.com.comsatel.clocator.microservice.posiciones.infrastructure.model.mysql.Historico;
import pe.com.comsatel.clocator.microservice.posiciones.shared.constans.Constants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static pe.com.comsatel.clocator.microservice.posiciones.shared.constans.Constants.*;

@Service
public class PosicionSatelitalTrackSolidServiceImpl implements DomainSatelitalTrackSolidService {
    private final static Logger LOG = LogManager.getLogger(PosicionSatelitalTrackSolidServiceImpl.class);

    // Variables JC450
    private static final String URLTrackSoLid = "us-open.tracksolidpro.com";
    private static final int MAX_INTENTS = 1;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> scheduledFuture;
    private int reintentos = 1;
    private String horaInicioParalizacion = null;
    private final Map<String, Integer> alertDictionary = new HashMap<>();
    private final AtomicBoolean hiloEnEjecucion = new AtomicBoolean(false);

    
    @Value("${skywave.position.host}")
    public String SKYWAVE_ACCESS_HOST;

    @Value("${skywave.position.port}")
    public String SKYWAVE_ACCESS_PORT;

    @Autowired
    @Qualifier("MYSQL")
    private PosicionesRepository domainRepository;

    @Autowired
    @Qualifier("MYSQL")
    private ProveedoresRepository proveedoresRepository;


    @Autowired
    private PositionTagMessageUseCase positionTagMessageUseCase;


    private List<PosicionSatelitalResponse> generatePosicionSatelitalResponse(String response) {
        List<PosicionSatelitalResponse> lstResponse = new ArrayList<>();
        lstResponse.add(PosicionSatelitalResponse.builder().resultado(response).build());
        return lstResponse;
    }

    // <editor-fold defaultstate="collapsed" desc="Envio de tramas">
    /**
     * Envia las tramas con la informacion de Tracsolid a Clocator
     * 
     * @param mensajes: Lista de mensajes Tracsolid que se enviaran
     */
    private void enviarMensajesTCP(List<ReturnMessageTrackSolid> mensajes) throws Exception {
        try {
            Socket socket;
            DataOutputStream outputStream;

            socket = new Socket(SKYWAVE_ACCESS_HOST, Integer.parseInt(SKYWAVE_ACCESS_PORT));
            socket.setSoTimeout(60000);
            LOG.debug(String.format("Socket: %s:%s", SKYWAVE_ACCESS_HOST, SKYWAVE_ACCESS_PORT));
            outputStream = new DataOutputStream(socket.getOutputStream());

            for (ReturnMessageTrackSolid msg : mensajes) {

                ProtocoloJC450Type protocolo = new ProtocoloJC450Type();
                protocolo.Parse(msg);

                LOG.debug("Trama JC450: " + protocolo.getTrama());
                outputStream.write(protocolo.getTrama().getBytes());
                outputStream.flush();
            }

            outputStream.close();
            socket.close();
        } catch (SocketException ex) {
            LOG.error("<<SocketException - " + ex.getMessage());
            throw ex;
        } catch (SocketTimeoutException ex) {
            LOG.error("<<SocketTimeoutException - " + ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            LOG.error("Exception en enviarMensajesTCP: {}", ex.getMessage());
            throw ex;
        }
    }

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Flujo Get TAG">
    @Override
    @Scheduled(fixedRate = 60000)
    public void scheduledPorMinutoTrackSolidPB750() {
        LOG.info("TrackSolid PB750 - Inicio de obtencion de posiciones por minuto");
        try {
            // Consumir Servicio TrackSolid PB750
            this.procesarPosicionTrackSolidPB750();
        } catch (ApplicationException e) {
            LOG.error("TrackSolid PB750 - Error en: {}", e.getMessage());
        } catch (Exception e) {
            LOG.error("TrackSolid PB750 - Error en: {}", e.getMessage());
        }        
        LOG.info("TrackSolid PB750 - Fin de obtencion de posiciones por minuto");

    }

    private void procesarPosicionTrackSolidPB750() throws ApplicationException{

        // Obtener credencial
        LOG.info("TrackSolid PB750 - Obteniendo Credenciales");
        Credencial credencial = proveedoresRepository.obtenerCredencialesTrackSolidPorId(CREDENCIAL_TRACK_SOLID_PERU);
    
        LOG.info("TrackSolid PB750 - Obteniendo Configuración de Servidor");
        Map<String, String>  servidor = this.obtenerServidor();

        LOG.info("TrackSolid PB750 - Obteniendo IMEIS para TAG MSG");
        List<String> imeis = this.obtenerImeisTagMsg();

        LOG.info("TrackSolid PB750 - Obteniendo AccessToken");
        String timestamp = obtenerTimeStampGMTFormat();
        TokenResponseTrackSolid accessToken = this.obtenerToken(timestamp, credencial);
        
        LOG.info("TrackSolid PB750 - Procesando TAG MSG");
        positionTagMessageUseCase.processPositionTagMsg(accessToken, imeis, credencial, servidor);

    }

    /**
     * Obtiene todos los Imeis de los dispositivos con Protocolo PB750
     * 
     * @return Lista de Imeis de los dispositivos.
     */
    private List<String> obtenerImeisTagMsg() throws ApplicationException{
        List<String> imeis = new ArrayList<>();

        try {
            imeis = domainRepository.obtenerImeisPB750();
        } catch (Exception ex) {
            LOG.error("Error en método obtenerImeisTagMsg(): {}", ex.getMessage());

            throw new ApplicationException("Error al obtener los IMEIS de los dispositivos PB750");
        }
        LOG.info("TrackSolid PB750 - IMEIs activos: " + imeis );//CP03
        return imeis;
    }

    /**
     * Obtiene servidor que se usará para enviar posiciones
     * 
     * @return Mapa con IP y Puerto del servidor
     */
    private Map<String, String> obtenerServidor() throws ApplicationException{
        String COD_SERVIDOR = "";
        String configuracion = "";
        Map<String, String> servidor = new HashMap<>();

        try {
            COD_SERVIDOR = domainRepository.getPropiedadAppValue(Constants.LLAVE_CODIGO_SERVIDOR).getValor();

            configuracion = domainRepository.getPropiedadAppValue(Constants.LLAVE_CONFIGURACION_SERVIDORES).getValor();

            if (configuracion != null && !configuracion.trim().isEmpty()) {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Map<String, String>> servidores = mapper.readValue(configuracion, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Map<String,String>>>() {});
                
                servidor = servidores.getOrDefault(COD_SERVIDOR, null);

                if(servidor != null){
                    LOG.info("TrackSolid PB750 - IP: " + servidor.get(POSITION_SERVER_HOST));
                    LOG.info("TrackSolid PB750 - Puerto: " + servidor.get(POSITION_SERVER_PORT));
                    return servidor;
                } else {
                    throw new ApplicationException("Configuración del servidor TrackSolid no encontrada para el código: " + COD_SERVIDOR);
                }

            } else {
                throw new ApplicationException("Configuración del servidor TrackSolid no encontrada");
            }

        } catch (Exception ex) {
            LOG.error("Error en método obtenerServidor(): {}", ex.getMessage());
            throw new ApplicationException("Error al obtener la configuración del servidor TrackSolid");
        }
    }
    
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Flujo Obtencion de posiciones">
    //@Override
    //@Scheduled(fixedRate = 60000)
    public void obtenerPosicionPorMinutoTrackSolid() {

        Boolean trackSolidEnabled = Boolean.parseBoolean(getPropiedad("TrackSolidEnabled"));
        if (trackSolidEnabled) {
            this.generarAlertasDict();
            iniciarConeccion();
        }
    }

    /**
     * Metodo para controlar las conecciones a TRACKSOLID y soprote ante caidas.
     */
    private void intentarObtenerPosicionPorMinuto() {
        if (hiloEnEjecucion.compareAndSet(false, true)) {
            try {
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    // Se obtiene todas las credenciales
                    List<Credencial> lstCredenciales = obtenerCredencialesTrackSolid();

                    for (Credencial credencial : lstCredenciales) {
                        try {
                            LOG.debug("Se procesara los vehiculos de {}", credencial.getUsuario());
                            boolean exitoso = true;
                            while (true) {
                                List<String> imeis = this.obtenerImeisClocator();
                                String horaFinParalizacion = null;
                                if (!this.conectarConTrackSolid(imeis, credencial)) {
                                    exitoso = false;
                                    if (this.horaInicioParalizacion == null)
                                        this.horaInicioParalizacion = obtenerTimeStampGMTFormat();
                                    if (this.reintentos == MAX_INTENTS) {
                                        this.reintentos = 1;
                                        this.detenerConeccion();
                                        this.scheduledExecutorService.schedule(this::reiniciarConeccion, 10,
                                                TimeUnit.MINUTES);
                                        break;
                                    }
                                    if (this.reintentos < MAX_INTENTS)
                                        this.reintentos++;
                                } else {
                                    exitoso = true;
                                    if (this.horaInicioParalizacion != null) {
                                        horaFinParalizacion = obtenerTimeStampGMTFormat();

                                        LocalDateTime fechaInicio = LocalDateTime.parse(this.horaInicioParalizacion,
                                                formatter);
                                        LocalDateTime fechaFin = LocalDateTime.parse(horaFinParalizacion, formatter);
                                        Duration duration = Duration.between(fechaInicio, fechaFin);
                                        Duration limite = Duration.ofHours(0).plusMinutes(1);

                                        if (duration.compareTo(limite) >= 1) {
                                            obtenerTodasPosicionesTrackSolid(this.horaInicioParalizacion,
                                                    horaFinParalizacion, credencial);
                                        }
                                    }
                                    break;
                                }
                            }
                            if (exitoso) {
                                this.reintentos = 1;
                                this.horaInicioParalizacion = null;
                                LOG.info("Se procesaron correctamente todos los Imeis");
                            }
                        } catch (Exception ex) {
                            LOG.error("Excepcion Generada", ex.getMessage(), ex);
                        }
                    }

                } catch (Exception ex) {
                    LOG.error("Excepcion Generada", ex.getMessage(), ex);
                }
            } finally {
                hiloEnEjecucion.set(false);
            }
        } else {
            LOG.warn("Se saltó una ejecución porque la anterior aún no termina.");
        }
    }

    /**
     * Inicia la coneccion a TRACKSOLID.
     */
    private void iniciarConeccion() {
        if (scheduledFuture == null || scheduledFuture.isCancelled()) {
            scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(this::intentarObtenerPosicionPorMinuto,
                    0, 1, TimeUnit.MINUTES);
            LOG.warn("Inicio consultas a TRACKSOLID.");
        }
    }

    /**
     * Reinicia la coneccion a TRACKSOLID.
     */
    private void reiniciarConeccion() {
        if (scheduledFuture == null || scheduledFuture.isCancelled()) {
            scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(this::intentarObtenerPosicionPorMinuto,
                    0, 1, TimeUnit.MINUTES);
            LOG.warn("Reinicio consultas a TRACKSOLID.");
        }
    }

    /**
     * Detiene la coneccion a TRACKSOLID.
     */
    private void detenerConeccion() {
        if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
            scheduledFuture.cancel(true);
            LOG.warn("Se detuvieron consultas a TRACKSOLID.");
        }
    }

    /**
     * Conecta con la api de Tracsolid para enviar las posiciones segun los IMEIS de
     * CLOCATOR
     * 
     * @param imeis: Lista de IMEIS de CLOCATOR con protocolo JC450
     * @return Indica si se logro conectar o no.
     */
    private boolean conectarConTrackSolid(List<String> imeis, Credencial credencial) {
        String method = "jimi.device.location.get";
        String signMethod = "md5";
        String version = "1.0";
        String format = "json";
        String timestamp = obtenerTimeStampGMTFormat();
        String timestampAnterior = obtenerTimeStampGMTFormatMenosUnMinuto();
        String map_type = "GOOGLE";

        try {
            TokenResponseTrackSolid accessToken = this.obtenerToken(timestamp, credencial);
            List<ReturnMessageTrackSolid> lstMessages = new ArrayList<>();

            if (accessToken.getCodigo() == 0) {
                HashMap<String, String> data = new HashMap<>();
                data.put("method", method);
                data.put("timestamp", timestamp);
                data.put("app_key", parsearCredencial(credencial, "TRACKSOLID_APP_KEY"));
                data.put("sign_method", signMethod);
                data.put("v", version);
                data.put("format", format);
                data.put("access_token", accessToken.getToken());
                data.put("imeis", String.join(",", imeis));
                data.put("map_type", map_type);

                String sign = generarFirma(data, parsearCredencial(credencial, "TRACKSOLID_APP_SECRET"));

                String queryParams = "method=" + method +
                        "&timestamp=" + timestamp +
                        "&app_key=" + parsearCredencial(credencial, "TRACKSOLID_APP_KEY") +
                        "&sign=" + sign +
                        "&sign_method=" + signMethod +
                        "&v=" + version +
                        "&format=" + format +
                        "&access_token=" + accessToken.getToken() +
                        "&imeis=" + String.join(",", imeis) +
                        "&map_type=" + map_type;

                LOG.debug("FIRMA: " + sign);
                LOG.debug("TOKEN: " + accessToken.getToken());
                LOG.debug("https://" + URLTrackSoLid + "/route/rest");
                LOG.debug(queryParams);

                HttpResponse<String> response = this.generarConsulta(queryParams);

                Gson gson = new Gson();
                if (response.statusCode() == 200) {
                    String responseBody = response.body();
                    JsonObject jsonResponse = gson.fromJson(responseBody, JsonObject.class);
                    if (jsonResponse.has("code")) {
                        int responseCode = jsonResponse.get("code").getAsInt();
                        if (responseCode == 0) {
                            JsonArray posiciones = jsonResponse.getAsJsonArray("result");
                            Gson gsonPos = new Gson();
                            for (JsonElement posicion : posiciones) {
                                ReturnMessageTrackSolid returnMessageTrackSolid = gsonPos.fromJson(posicion,
                                        ReturnMessageTrackSolid.class);
                                List<ReturnEventTrackSolid> idEventos = obetenerEventos(
                                        returnMessageTrackSolid.getImei(), accessToken.getToken(),
                                        timestampAnterior, timestamp, credencial);
                                if (idEventos.isEmpty()) {
                                    // ACA DESCOMENTAR PARA OBTENER LOS EVENTOS EN PRUEBAS SIEMPRE Y CUANDO
                                    // TRACKSOLID NO RETORNE NINGUN EVENTO
                                    /*
                                     * for (int valor : alertDictionary.values()) {
                                     * returnMessageTrackSolid.setIdEvento(valor);
                                     * returnMessageTrackSolid.setGpsTime(obtenerTimeStampGMTFormat());
                                     * lstMessages.add(returnMessageTrackSolid);
                                     * }
                                     */
                                    // COMENTAR ACA PARA PRUEBAS
                                    returnMessageTrackSolid.setIdEvento(0);
                                    lstMessages.add(returnMessageTrackSolid);
                                } else {
                                    for (ReturnEventTrackSolid evento : idEventos) {
                                        returnMessageTrackSolid.setIdEvento(evento.getIdEvento());
                                        returnMessageTrackSolid.setLatitud(evento.getLatitud());
                                        returnMessageTrackSolid.setLongitud(evento.getLongitud());
                                        returnMessageTrackSolid.setGpsTime(evento.getGpsTime());
                                        lstMessages.add(returnMessageTrackSolid);
                                    }
                                }
                            }
                            this.enviarMensajesTCP(lstMessages);
                        }
                        if (responseCode >= 1001) {
                            String message = jsonResponse.get("message").getAsString();
                            LOG.error("Error jimi.device.location.get: {} : {}", responseCode, message);
                            return false;
                        }
                    }
                }
                return true;
            }
        } catch (Exception e) {
            LOG.error("Error en método conectarConTrackSolid(): {}", e.getMessage());
            e.printStackTrace();

        }
        return false;
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Consultas Extras">
    /**
     * Obtiene el Token de acceso al API de TracsolidPro ya sea desde BD o desde el
     * API jimi.oauth.token.get
     * 
     * @param timestamp: Fecha actual en formato yyyy-MM-dd HH:mm:ss
     * @return Objeto del tipo TokenResponseTrackSolid con el token y el estado de
     *         la consulta.
     */
    private TokenResponseTrackSolid obtenerToken(String timestamp, Credencial credencial) {
        TokenResponseTrackSolid responseToken = new TokenResponseTrackSolid();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try {
            // Obtener propiedades
            String trackSolidAcToken = credencial.getRefreshToken();
            String trackSolidTimeToken = credencial.getRefreshTokenTime();
            String trackSolidDurationToken = credencial.getRefreshTokenDuration();

            // Si faltan las propiedades, se asume que se debe generar un nuevo token
            boolean requiereNuevoToken = (trackSolidAcToken == null || trackSolidAcToken.trim().isEmpty()
                    || trackSolidTimeToken == null || trackSolidDurationToken == null);

            if (!requiereNuevoToken) {
                // Validar duración del token
                long durationLimitSeconds = Long.parseLong(trackSolidDurationToken);
                LocalDateTime fechaServer = LocalDateTime.parse(trackSolidTimeToken, formatter);
                LocalDateTime fechaActual = LocalDateTime.parse(obtenerTimeStampGMTFormat(), formatter);
                long elapsedSeconds = Duration.between(fechaServer, fechaActual).getSeconds();

                // Si el token actual es válido, devolverlo
                if (elapsedSeconds < durationLimitSeconds) {
                    responseToken.setCodigo(0);
                    responseToken.setToken(trackSolidAcToken);
                    return responseToken;
                }
            }

            // Generar un nuevo token
            String method = "jimi.oauth.token.get";
            String signMethod = "md5";
            String version = "1.0";
            String format = "json";
            String expiresIn = "7200";

            HashMap<String, String> data = new HashMap<>();
            data.put("method", method);
            data.put("timestamp", timestamp);
            data.put("app_key", parsearCredencial(credencial, "TRACKSOLID_APP_KEY"));
            data.put("sign_method", signMethod);
            data.put("v", version);
            data.put("format", format);
            data.put("user_id", parsearCredencial(credencial, "TRACKSOLID_USER_ID"));
            data.put("user_pwd_md5", parsearCredencial(credencial, "TRACKSOLID_USER_PWD_MD5"));
            data.put("expires_in", expiresIn);

            String sign = generarFirma(data, parsearCredencial(credencial, "TRACKSOLID_APP_SECRET"));

            String queryParams = "method=" + URLEncoder.encode(method, StandardCharsets.UTF_8) +
                    "&timestamp=" + URLEncoder.encode(timestamp, StandardCharsets.UTF_8) +
                    "&app_key=" + parsearCredencial(credencial, "TRACKSOLID_APP_KEY") +
                    "&sign=" + sign +
                    "&sign_method=" + signMethod +
                    "&v=" + version +
                    "&format=" + format +
                    "&user_id=" + parsearCredencial(credencial, "TRACKSOLID_USER_ID") +
                    "&user_pwd_md5=" + parsearCredencial(credencial, "TRACKSOLID_USER_PWD_MD5") +
                    "&expires_in=" + expiresIn;

            LOG.debug("FIRMA: " + sign);
            LOG.debug("PARAMS: " + queryParams);
            LOG.debug("https://" + URLTrackSoLid + "/route/rest");

            HttpResponse<String> response = this.generarConsulta(queryParams);

            if (response.statusCode() == 200) {
                String responseBody = response.body();
                LOG.info("Respuesta exitosa {}", responseBody);
                Gson gson = new Gson();
                JsonObject jsonResponse = gson.fromJson(responseBody, JsonObject.class);
                if (jsonResponse.has("code")) {
                    int responseCode = jsonResponse.get("code").getAsInt();
                    responseToken.setCodigo(responseCode);
                    if (responseCode == 0) {
                        JsonObject result = jsonResponse.getAsJsonObject("result");
                        String accessToken = result.get("accessToken").getAsString();
                        String fechaCreacionToken = result.get("time").getAsString();
                        responseToken.setToken(accessToken);

                        // Guardar nuevo token y su fecha de creación
                        credencial.setRefreshToken(accessToken);
                        credencial.setRefreshTokenDuration(expiresIn);
                        credencial.setRefreshTokenTime(fechaCreacionToken);
                        actualizarCredencialesTrackSolid(credencial);
                    } else if (responseCode >= 1001) {
                        String message = jsonResponse.get("message").getAsString();
                        LOG.error("Error jimi.oauth.token.get: {} : {}", responseCode, message);
                        responseToken.setMensaje(message);
                    }
                }
            } else {
                LOG.error("Error en la solicitud del token. Código de estado: {}", response.statusCode());
                responseToken.setCodigo(-1);
                responseToken.setMensaje("Error al generar un nuevo token.");
            }

        } catch (Exception e) {
            LOG.error("Error en método ObtenerToken(): {}", e.getMessage(), e);
            responseToken.setCodigo(-2);
            responseToken.setMensaje(e.getMessage());
        }

        return responseToken;
    }

    /**
     * Obtiene las posicions en un rango de horas y fechas
     * 
     * @param begin_time: Fecha y hora de inicio de la paralización
     * @param end_time:   Fecha y hora de fin de la paralización
     * @return Lista de posiciones obtenidas
     */
    private List<PosicionSatelitalResponse> obtenerTodasPosicionesTrackSolid(String begin_time, String end_time,
            Credencial credencial) {
        String method = "jimi.device.track.list";
        String signMethod = "md5";
        String version = "1.0";
        String format = "json";
        String timestamp = obtenerTimeStampGMTFormat();
        String map_type = "GOOGLE";

        List<String> imeis = this.obtenerImeisClocator();

        StringBuilder responsePosiciones = new StringBuilder();
        List<ReturnMessageTrackSolid> lstMessages = new ArrayList<>();

        try {

            TokenResponseTrackSolid accessToken = this.obtenerToken(timestamp, credencial);

            if (accessToken.getCodigo() == 0) {
                for (String imei : imeis) {
                    HashMap<String, String> data = new HashMap<>();
                    data.put("method", method);
                    data.put("timestamp", timestamp);
                    data.put("app_key", parsearCredencial(credencial, "TRACKSOLID_APP_KEY"));
                    data.put("sign_method", signMethod);
                    data.put("v", version);
                    data.put("format", format);
                    data.put("access_token", accessToken.getToken());
                    data.put("imei", imei);
                    data.put("map_type", map_type);
                    data.put("begin_time", begin_time);
                    data.put("end_time", end_time);

                    String sign = generarFirma(data, parsearCredencial(credencial, "TRACKSOLID_APP_SECRET"));

                    String queryParams = "method=" + URLEncoder.encode(method, StandardCharsets.UTF_8) +
                            "&timestamp=" + URLEncoder.encode(timestamp, StandardCharsets.UTF_8) +
                            "&app_key=" + parsearCredencial(credencial, "TRACKSOLID_APP_KEY") +
                            "&sign=" + sign +
                            "&sign_method=" + signMethod +
                            "&v=" + version +
                            "&format=" + format +
                            "&access_token=" + accessToken.getToken() +
                            "&imei=" + imei +
                            "&begin_time=" + URLEncoder.encode(begin_time, StandardCharsets.UTF_8) +
                            "&end_time=" + URLEncoder.encode(end_time, StandardCharsets.UTF_8) +
                            "&map_type=" + map_type;

                    HttpResponse<String> response = this.generarConsulta(queryParams);

                    Gson gson = new Gson();
                    if (response.statusCode() == 200) {
                        String responseBody = response.body();
                        JsonObject jsonResponse = gson.fromJson(responseBody, JsonObject.class);
                        if (jsonResponse.has("code")) {
                            int responseCode = jsonResponse.get("code").getAsInt();
                            if (responseCode == 0) {
                                JsonArray posiciones = jsonResponse.getAsJsonArray("result");

                                Gson gsonPos = new Gson();

                                for (JsonElement posicion : posiciones) {
                                    ReturnMessageTrackSolid returnMessageTrackSolid = gsonPos.fromJson(posicion,
                                            ReturnMessageTrackSolid.class);
                                    List<ReturnEventTrackSolid> idEventos = obetenerEventos(
                                            returnMessageTrackSolid.getImei(), accessToken.getToken(),
                                            begin_time, end_time, credencial);
                                    if (idEventos.isEmpty()) {
                                        returnMessageTrackSolid.setIdEvento(0);
                                        lstMessages.add(returnMessageTrackSolid);
                                    } else {
                                        for (ReturnEventTrackSolid evento : idEventos) {
                                            returnMessageTrackSolid.setIdEvento(evento.getIdEvento());
                                            returnMessageTrackSolid.setLatitud(evento.getLatitud());
                                            returnMessageTrackSolid.setLongitud(evento.getLongitud());
                                            returnMessageTrackSolid.setGpsTime(evento.getGpsTime());
                                            lstMessages.add(returnMessageTrackSolid);
                                        }
                                    }
                                }

                                this.enviarMensajesTCP(lstMessages);
                                responsePosiciones
                                        .append(String.format("Se procesaron %s posiciones", posiciones.size()));
                            }
                            if (responseCode >= 1001) {
                                String message = jsonResponse.get("message").getAsString();
                                responsePosiciones.append(String.format("Error jimi.device.track.list: %s", message));
                            }
                        }
                    }
                }
            } else {
                responsePosiciones = new StringBuilder(accessToken.getMensaje());
            }
            return generatePosicionSatelitalResponse(responsePosiciones.toString());
        } catch (Exception e) {
            LOG.error("Error en método obtenerTodasPosicionesTrackSolid(): {}", e.getMessage());
            e.printStackTrace();
            return generatePosicionSatelitalResponse(e.getMessage());
        }
    }

    /**
     * Genera la consulta a TrackSolid
     * 
     * @param queryParams: Parametros de la consulta a TrackSolid
     * @return Respuesta en Formato String
     */
    private HttpResponse<String> generarConsulta(String queryParams)
            throws IOException, InterruptedException, URISyntaxException {
        URI uri = new URI("https://" + URLTrackSoLid + "/route/rest");

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(queryParams))
                .build();

        LOG.debug("request: {}", request);
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        LOG.debug("response: {}", response.body());
        return response;
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Eventos">
    /**
     * Conecta con la api de Tracsolid para enviar las posiciones segun los IMEIS de
     * CLOCATOR
     * 
     * @param imei: Lista de IMEIS de CLOCATOR con protocolo JC450
     * @return Indica si se logro conectar o no.
     */
    private List<ReturnEventTrackSolid> obetenerEventos(String imei, String token, String begin_time, String end_time,
            Credencial credencial) {
        String method = "jimi.device.alarm.list";
        String signMethod = "md5";
        String version = "1.0";
        String format = "json";
        String timestamp = obtenerTimeStampGMTFormat();
        List<ReturnEventTrackSolid> eventos = new ArrayList<>();

        try {
            HashMap<String, String> data = new HashMap<>();
            data.put("method", method);
            data.put("timestamp", timestamp);
            data.put("app_key", parsearCredencial(credencial, "TRACKSOLID_APP_KEY"));
            data.put("sign_method", signMethod);
            data.put("v", version);
            data.put("format", format);
            data.put("access_token", token);
            data.put("imei", imei);
            data.put("begin_time", begin_time);
            data.put("end_time", end_time);

            String sign = generarFirma(data, parsearCredencial(credencial, "TRACKSOLID_APP_SECRET"));

            String queryParams = "method=" + URLEncoder.encode(method, StandardCharsets.UTF_8) +
                    "&timestamp=" + URLEncoder.encode(timestamp, StandardCharsets.UTF_8) +
                    "&app_key=" + parsearCredencial(credencial, "TRACKSOLID_APP_KEY") +
                    "&sign=" + sign +
                    "&sign_method=" + signMethod +
                    "&v=" + version +
                    "&format=" + format +
                    "&access_token=" + token +
                    "&imei=" + imei +
                    "&begin_time=" + URLEncoder.encode(begin_time, StandardCharsets.UTF_8) +
                    "&end_time=" + URLEncoder.encode(end_time, StandardCharsets.UTF_8);

            HttpResponse<String> response = this.generarConsulta(queryParams);

            Gson gson = new Gson();
            if (response.statusCode() == 200) {
                String responseBody = response.body();
                JsonObject jsonResponse = gson.fromJson(responseBody, JsonObject.class);
                if (jsonResponse.has("code")) {
                    int responseCode = jsonResponse.get("code").getAsInt();
                    if (responseCode == 0) {
                        if (jsonResponse.has("result") && !jsonResponse.get("result").isJsonNull()) {
                            JsonArray eventosTs = jsonResponse.getAsJsonArray("result");
                            Gson gsonPos = new Gson();
                            if (eventosTs != null) {
                                for (JsonElement eventoTs : eventosTs) {
                                    ReturnEventTrackSolid returnMessageTrackSolid = gsonPos.fromJson(eventoTs,
                                            ReturnEventTrackSolid.class);
                                    returnMessageTrackSolid.setIdEvento(
                                            this.obtenerIdEvento(returnMessageTrackSolid.getAlarmTypeId()));
                                    eventos.add(returnMessageTrackSolid);
                                }
                            }
                        }
                    }
                    if (responseCode >= 1001) {
                        String message = jsonResponse.get("message").getAsString();
                        LOG.error("Error jimi.device.alarm.list: {} : {}", responseCode, message);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error en método obetenerEventos(): {}", e.getMessage());
            e.printStackTrace();
        }
        return eventos;
    }

    /**
     * Retorna el Id correspondiente segun el tipo de evento obtenido en TRACKSOLID
     * 
     * @param alertId: Id del evento obtenido en TRACKSOLID
     * @return Id del evento correspondiente en CLocator
     */
    public int obtenerIdEvento(String alertId) {
        return alertDictionary.get(alertId);
    }

    /**
     * Genera la lista de eventos en un diccionario
     */
    private void generarAlertasDict() {
        alertDictionary.put("ubiTurn", 279);
        alertDictionary.put("ubiStab", 280);
        alertDictionary.put("ubiSatt", 281);
        alertDictionary.put("ubiRoll", 282);
        alertDictionary.put("ubiLane", 283);
        alertDictionary.put("ubiDece", 284);
        alertDictionary.put("ubiColl", 285);
        alertDictionary.put("ubiAcce", 286);
        alertDictionary.put("stayTimeOut", 287);
        alertDictionary.put("stayTimeIn", 288);
        alertDictionary.put("stayAlertOn", 289);
        alertDictionary.put("stayAlert", 290);
        alertDictionary.put("statusTrunk_1", 291);
        alertDictionary.put("statusTrunk_0", 292);
        alertDictionary.put("statusRightRearWindows_1", 293);
        alertDictionary.put("statusRightRearWindows_0", 294);
        alertDictionary.put("statusRightRearDoors_1", 295);
        alertDictionary.put("statusRightRearDoors_0", 296);
        alertDictionary.put("statusRightFrontWindows_1", 297);
        alertDictionary.put("statusRightFrontWindows_0", 298);
        alertDictionary.put("statusRightFrontDoors_1", 299);
        alertDictionary.put("statusRightFrontDoors_0", 300);
        alertDictionary.put("statusLeftRearWindows_1", 301);
        alertDictionary.put("statusLeftRearWindows_0", 302);
        alertDictionary.put("statusLeftFrontWindows_1", 303);
        alertDictionary.put("statusLeftFrontWindows_0", 304);
        alertDictionary.put("statusLeftFrontDoors_1", 305);
        alertDictionary.put("statusLeftFrontDoors_0", 306);
        alertDictionary.put("sensitiveAreasFence", 307);
        alertDictionary.put("overSpeed", 308);
        alertDictionary.put("out", 309);
        alertDictionary.put("other", 310);
        alertDictionary.put("offline", 311);
        alertDictionary.put("obd", 312);
        alertDictionary.put("mileageAlarm", 313);
        alertDictionary.put("low_temp_alarm", 314);
        alertDictionary.put("laneshift", 315);
        alertDictionary.put("inEnter", 316);
        alertDictionary.put("high_temp_alarm", 317);
        alertDictionary.put("geozone", 318);
        alertDictionary.put("fenceOverspeed", 319);
        alertDictionary.put("drivingBehaviorAlertDVR", 320);
        alertDictionary.put("drivingBehaviorAlert", 321);
        alertDictionary.put("DMSAlert", 322);
        alertDictionary.put("displacementAlarm", 323);
        alertDictionary.put("carFault", 324);
        alertDictionary.put("burglarStatus_2", 325);
        alertDictionary.put("burglarStatus_1", 326);
        alertDictionary.put("burglarStatus_0", 327);
        alertDictionary.put("ACC_ON", 328);
        alertDictionary.put("ACC_OFF", 329);
        alertDictionary.put("268", 278);
        alertDictionary.put("267", 277);
        alertDictionary.put("266", 276);
        alertDictionary.put("263", 273);
        alertDictionary.put("262", 272);
        alertDictionary.put("261", 271);
        alertDictionary.put("260", 270);
        alertDictionary.put("259", 269);
        alertDictionary.put("258", 268);
        alertDictionary.put("257", 267);
        alertDictionary.put("256", 266);
        alertDictionary.put("254", 264);
        alertDictionary.put("233", 243);
        alertDictionary.put("232", 242);
        alertDictionary.put("231", 241);
        alertDictionary.put("230", 240);
        alertDictionary.put("227", 237);
        alertDictionary.put("224", 234);
        alertDictionary.put("208", 218);
        alertDictionary.put("207", 217);
        alertDictionary.put("206", 216);
        alertDictionary.put("205", 215);
        alertDictionary.put("204", 214);
        alertDictionary.put("203", 213);
        alertDictionary.put("202", 212);
        alertDictionary.put("199", 209);
        alertDictionary.put("198", 208);
        alertDictionary.put("197", 207);
        alertDictionary.put("191", 201);
        alertDictionary.put("173", 183);
        alertDictionary.put("172", 182);
        alertDictionary.put("171", 181);
        alertDictionary.put("170", 180);
        alertDictionary.put("169", 179);
        alertDictionary.put("168", 178);
        alertDictionary.put("165", 175);
        alertDictionary.put("163", 173);
        alertDictionary.put("160", 170);
        alertDictionary.put("154", 164);
        alertDictionary.put("151", 161);
        alertDictionary.put("150", 160);
        alertDictionary.put("149", 159);
        alertDictionary.put("148", 158);
        alertDictionary.put("147", 157);
        alertDictionary.put("146", 156);
        alertDictionary.put("145", 155);
        alertDictionary.put("144", 154);
        alertDictionary.put("143", 153);
        alertDictionary.put("142", 152);
        alertDictionary.put("141", 151);
        alertDictionary.put("140", 150);
        alertDictionary.put("139", 149);
        alertDictionary.put("138", 148);
        alertDictionary.put("136", 146);
        alertDictionary.put("135", 145);
        alertDictionary.put("128", 138);
        alertDictionary.put("127", 137);
        alertDictionary.put("126", 136);
        alertDictionary.put("120", 130);
        alertDictionary.put("119", 129);
        alertDictionary.put("115", 125);
        alertDictionary.put("114", 124);
        alertDictionary.put("113", 123);
        alertDictionary.put("101", 111);
        alertDictionary.put("100", 110);
        alertDictionary.put("92", 102);
        alertDictionary.put("91", 101);
        alertDictionary.put("90", 100);
        alertDictionary.put("89", 99);
        alertDictionary.put("87", 97);
        alertDictionary.put("86", 96);
        alertDictionary.put("83", 93);
        alertDictionary.put("82", 92);
        alertDictionary.put("79", 89);
        alertDictionary.put("78", 88);
        alertDictionary.put("77", 87);
        alertDictionary.put("76", 86);
        alertDictionary.put("71", 81);
        alertDictionary.put("58", 68);
        alertDictionary.put("50", 60);
        alertDictionary.put("48", 58);
        alertDictionary.put("45", 55);
        alertDictionary.put("44", 54);
        alertDictionary.put("43", 53);
        alertDictionary.put("42", 52);
        alertDictionary.put("41", 51);
        alertDictionary.put("40", 50);
        alertDictionary.put("39", 49);
        alertDictionary.put("36", 46);
        alertDictionary.put("35", 45);
        alertDictionary.put("28", 38);
        alertDictionary.put("25", 35);
        alertDictionary.put("24", 34);
        alertDictionary.put("22", 32);
        alertDictionary.put("21", 31);
        alertDictionary.put("20", 30);
        alertDictionary.put("19", 29);
        alertDictionary.put("18", 28);
        alertDictionary.put("17", 27);
        alertDictionary.put("16", 26);
        alertDictionary.put("15", 25);
        alertDictionary.put("14", 24);
        alertDictionary.put("13", 23);
        alertDictionary.put("12", 22);
        alertDictionary.put("11", 21);
        alertDictionary.put("10", 20);
        alertDictionary.put("9", 19);
        alertDictionary.put("6", 16);
        alertDictionary.put("5", 15);
        alertDictionary.put("4", 14);
        alertDictionary.put("3", 13);
        alertDictionary.put("2", 12);
        alertDictionary.put("1", 11);
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Generación de la firma">
    /**
     * Generacion de la firma en fomato MD5 para el parametro sign en las consultas.
     * 
     * @param data:      Diccionario con datos de la firma.
     * @param appSecret: Clave secreta de TrackSolidPro.
     * @return Firma en foramto MD5 de 32 caracteres.
     */
    private static String generarFirma(Map<String, String> data, String appSecret) {
        List<String> keys = new ArrayList<>(data.keySet());
        Collections.sort(keys);

        StringBuilder concatenatedString = new StringBuilder(appSecret);
        for (String key : keys) {
            concatenatedString.append(key).append(data.get(key));
            LOG.debug("LLave: " + key);
            LOG.debug("Valor: " + data.get(key));
        }
        concatenatedString.append(appSecret);
        LOG.debug(concatenatedString);
        return generarMd5Hash(concatenatedString.toString());
    }

    /**
     * Convierte el string de la firma en formato MD5 de 32 caracteres.
     * 
     * @param input: Firma generada con el diccionario de datos.
     * @return Firma en foramto MD5 de 32 caracteres.
     */
    private static String generarMd5Hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(input.getBytes());

            StringBuilder hexString = new StringBuilder();
            for (byte b : messageDigest) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1)
                    hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString().toUpperCase();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Obtiene la fecha con la Zona GMT en formato yyyy-MM-dd HH:mm:ss
     * 
     * @return String con la fecha.
     */
    private static String obtenerTimeStampGMTFormat() {
        ZonedDateTime gmtTime = ZonedDateTime.now(ZoneId.of("GMT"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return gmtTime.format(formatter);
    }

    private static String obtenerTimeStampGMTFormatMenosUnMinuto() {
        ZonedDateTime gmtTime = ZonedDateTime.now(ZoneId.of("GMT")).minusMinutes(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return gmtTime.format(formatter);
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Base de datos">
    /**
     * Obtiene el valor de la propiedad enviada para la llave.
     * 
     * @param llave: Clave que se buscará en la tabla TB_PROPIEDADAPP
     * @return String con el valor de la propiedad.
     */
    private String getPropiedad(String llave) {
        try {
            PropiedadAppType propiedad = domainRepository.getPropiedadAppValue(llave);
            LOG.debug("Se obtiene propiedad {}: {}", llave, propiedad.getValor());
            return propiedad.getValor();
        } catch (Exception e) {
            LOG.error("Error en método getPropiedad ({}): {}", llave, e.getMessage());
            return null;
        }
    }

    private void actualizarCredencialesTrackSolid(Credencial credencial) {
        try {
            proveedoresRepository.actualizarCredencialesTrackSolid(credencial);
        } catch (Exception ex) {
            LOG.error("Excepcion Generada", ex.getMessage(), ex);
        }
    }

    private List<Credencial> obtenerCredencialesTrackSolid() {
        List<Credencial> lstCredenciales = new ArrayList<>();
        try {
            lstCredenciales = proveedoresRepository.obtenerCredencialesTrackSolid();
            LOG.info("Se obtuvo {} credencial(es)", lstCredenciales.size());
        } catch (Exception ex) {
            LOG.error("Excepcion Generada", ex.getMessage(), ex);
        }
        return lstCredenciales;
    }

    private String parsearCredencial(Credencial credencial, String llave) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> mapa = mapper.readValue(credencial.getCredenciales(), Map.class);
            return mapa.getOrDefault(llave, null);
        } catch (Exception ex) {
            LOG.error("Excepcion Generada", ex.getMessage(), ex);
            return null;
        }
    }

    /**
     * Actualiza el valor de una propiedad con un neuvo valor
     * 
     * @param llave: Nombre de la propeidad que se actgualziara en la tabla
     *               TB_PROPIEDADAPP.
     * @param valor: Valor de la propiedad que se actgualziara en la tabla
     *               TB_PROPIEDADAPP
     */
    private void setPropiedad(String llave, String valor) {
        try {
            LOG.debug("Se actualiza propiedad {}: {}", llave, valor);
            domainRepository.setPropiedadAppValue(llave, valor);
        } catch (Exception e) {
            LOG.error("Error en método setPropiedad: {}", e.getMessage());
        }
    }

    /**
     * Obtiene todos los Imeis de los dispositivos con Protocolo JC450
     * 
     * @return Lista de Imeis de los dispositivos.
     */
    private List<String> obtenerImeisClocator() {
        List<String> imeis = new ArrayList<>();

        try {
            imeis = domainRepository.obtenerImeisJC450();
        } catch (Exception ex) {
            LOG.error("Error en método obtenerImeisClocator(): {}", ex.getMessage());
        }
        return imeis;
    }

    

    /**
     * Genera los registros de la tabla TB_HISTORICO
     * 
     * @param lstMessages: Lista de respuestas obtenidas en TRACKSOLID
     * @return Registros que van en la tabla TB_HISTORICO
     */
    private List<Historico> generarHistorico(List<ReturnMessageTrackSolid> lstMessages) {
        List<Historico> historicos = new ArrayList<>();
        for (ReturnMessageTrackSolid mensaje : lstMessages) {
            DispositivoTrackSolid dispositivoVehiculoTrackSolid = domainRepository
                    .obtenerVehiculoDispositivo(mensaje.getImei());
            int idVehiculo = dispositivoVehiculoTrackSolid.getVehiculoId();
            int idDispositivo = dispositivoVehiculoTrackSolid.getDispositivoId();
            Historico elemento = new Historico();
            elemento.setLatitud(mensaje.getLatitud());
            elemento.setLongitud(mensaje.getLongitud());
            elemento.setVelocidad(mensaje.getGpsSpeed());
            elemento.setExactitud(1);
            elemento.setNroSatelite(mensaje.getGpsNum());
            elemento.setFechaInicio(mensaje.getGpsTime());
            elemento.setFechaFin(mensaje.getGpsTime());
            elemento.setRadio(0);
            elemento.setEventoId(mensaje.getIdEvento());
            elemento.setVehiculoId(idVehiculo);
            elemento.setDispositivoId(idDispositivo);
            elemento.setEstado("L");
            elemento.setDireccion("");
            elemento.setOdometro(0);
            elemento.setEstadoIgnition(mensaje.getIgnition());
            elemento.setEstadoValido(1);
            elemento.setDistancia(mensaje.getDistancia());
            elemento.setTipoLocalizacion(4);
            historicos.add(elemento);
        }

        return historicos;
    }
    // </editor-fold>
}
