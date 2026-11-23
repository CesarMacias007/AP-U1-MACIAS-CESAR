import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

public class Coordinador {
    private final int cupo;
    private final String tokenValido;
    private final Path rutaLog;

    private int emitidos = 0;
    private int atendidos = 0;
    private int reloj = 0;
    private final Queue<Integer> colaEspera = new LinkedList<>();

    public Coordinador(int cupo, String tokenValido) throws IOException {
        this.cupo = cupo;
        this.tokenValido = tokenValido;
        Files.createDirectories(Paths.get("logs"));
        this.rutaLog = Paths.get("logs", "coordinador.log");
        registrarLog("INICIO_COORDINADOR", "Coordinador iniciado con cupo=" + cupo);
    }

    private synchronized void registrarLog(String evento, String detalle) {
        reloj++;
        String linea = reloj + "; 0; " + evento + "; " + detalle + System.lineSeparator();
        try {
            Files.writeString(rutaLog, linea, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Error escribiendo en bitacora: " + e.getMessage());
        }
    }

    public synchronized String procesarSolicitud(String lineaJson) {
        Map<String, String> msg = MensajeJson.parsear(lineaJson);
        String tipo = msg.getOrDefault("tipo", "");
        String token = msg.getOrDefault("token", "");
        int vid = Integer.parseInt(msg.getOrDefault("ventanilla", "-1"));

        // Requerimiento A3: Validar token en toda solicitud y registrar rechazo
        if (token.isEmpty() || !tokenValido.equals(token)) {
            registrarLog("RECHAZO_TOKEN", "Solicitud " + tipo + " rechazada desde V" + vid + " por token invalido");
            System.out.println("[COORDINADOR] Rechazo por token invalido desde V" + vid);
            return MensajeJson.construir(
                    "tipo", "RESPUESTA",
                    "resultado", "RECHAZADA",
                    "motivo", "token invalido",
                    "lamport", reloj
            );
        }

        // Requerimiento A1: Procesar TURNO, LLAMAR y ESTADO
        switch (tipo) {
            case "TURNO" -> {
                if (emitidos >= cupo) {
                    registrarLog("TURNO_RECHAZADO", "Cupo agotado (" + cupo + ") solicitado por V" + vid);
                    return MensajeJson.construir(
                            "tipo", "RESPUESTA",
                            "resultado", "RECHAZADA",
                            "motivo", "cupo agotado",
                            "emitidos", emitidos,
                            "cupo", cupo,
                            "lamport", reloj
                    );
                }
                emitidos++;
                colaEspera.add(emitidos);
                registrarLog("TURNO_ASIGNADO", "Turno #" + emitidos + " entregado a V" + vid);
                System.out.println("[COORDINADOR] Turno #" + emitidos + " asignado a V" + vid);
                return MensajeJson.construir(
                        "tipo", "RESPUESTA",
                        "resultado", "ASIGNADO",
                        "numero", emitidos,
                        "emitidos", emitidos,
                        "cupo", cupo,
                        "lamport", reloj
                );
            }
            case "LLAMAR" -> {
                if (colaEspera.isEmpty()) {
                    registrarLog("LLAMAR_RECHAZADO", "No hay turnos en espera para V" + vid);
                    return MensajeJson.construir(
                            "tipo", "RESPUESTA",
                            "resultado", "RECHAZADA",
                            "motivo", "no hay turnos en espera",
                            "lamport", reloj
                    );
                }
                int turnoLlamado = colaEspera.poll();
                atendidos++;
                registrarLog("TURNO_LLAMADO", "Turno #" + turnoLlamado + " llamado por V" + vid);
                System.out.println("[COORDINADOR] V" + vid + " llama al turno #" + turnoLlamado);
                return MensajeJson.construir(
                        "tipo", "RESPUESTA",
                        "resultado", "LLAMADO",
                        "numero", turnoLlamado,
                        "emitidos", emitidos,
                        "cupo", cupo,
                        "lamport", reloj
                );
            }
            case "ESTADO" -> {
                registrarLog("ESTADO_CONSULTADO", "Estado enviado a V" + vid);
                return MensajeJson.construir(
                        "tipo", "RESPUESTA",
                        "resultado", "ESTADO",
                        "emitidos", emitidos,
                        "cupo", cupo,
                        "atendidos", atendidos,
                        "en_espera", new ArrayList<>(colaEspera),
                        "lamport", reloj
                );
            }
            default -> {
                registrarLog("SOLICITUD_INVALIDA", "Tipo desconocido: " + tipo);
                return MensajeJson.construir(
                        "tipo", "RESPUESTA",
                        "resultado", "RECHAZADA",
                        "motivo", "tipo de solicitud no soportado",
                        "lamport", reloj
                );
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            props.load(fis);
        }

        int puerto = Integer.parseInt(props.getProperty("puerto", "5000").trim());
        int cupo = Integer.parseInt(props.getProperty("cupo", "10").trim());
        String token = props.getProperty("token", "").trim();

        Coordinador coord = new Coordinador(cupo, token);

        try (ServerSocket serverSocket = new ServerSocket(puerto)) {
            System.out.println("[COORDINADOR 0] Escuchando en puerto " + puerto + " (Cupo=" + cupo + ")...");
            while (true) {
                Socket cliente = serverSocket.accept();
                new Thread(() -> manejarConexion(cliente, coord)).start();
            }
        }
    }

    private static void manejarConexion(Socket socket, Coordinador coord) {
        // Requerimiento A2: Delimitacion de mensajes por salto de linea (readLine / println)
        try (
                Socket s = socket;
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)
        ) {
            String linea;
            while ((linea = in.readLine()) != null) {
                if (!linea.isBlank()) {
                    String respuesta = coord.procesarSolicitud(linea);
                    out.println(respuesta);
                }
            }
        } catch (IOException ignored) {
        }
    }
}