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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Coordinador {
    private final int cupo;
    private final String tokenValido;
    private final boolean modoInseguro;
    private final Path rutaLog;

    private final Object lockTurnos = new Object();
    private int emitidos = 0;
    private int atendidos = 0;
    private int reloj = 0;
    private final Queue<Integer> colaEspera = new LinkedList<>();

    public Coordinador(int cupo, String tokenValido, boolean modoInseguro) throws IOException {
        this.cupo = cupo;
        this.tokenValido = tokenValido;
        this.modoInseguro = modoInseguro;
        Files.createDirectories(Paths.get("logs"));
        this.rutaLog = Paths.get("logs", "coordinador.log");

        String modo = modoInseguro ? "INSEGURO (pausa 50 ms)" : "NORMAL (con exclusion mutua)";
        registrarLog("INICIO_COORDINADOR", "Cupo=" + cupo + ", Modo=" + modo);
        System.out.println("[COORDINADOR 0] Iniciado | Cupo=" + cupo + " | Modo=" + modo);
    }

    private synchronized int registrarLog(String evento, String detalle) {
        reloj++;
        String linea = reloj + "; 0; " + evento + "; " + detalle + System.lineSeparator();
        try {
            Files.writeString(rutaLog, linea, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Error escribiendo bitacora: " + e.getMessage());
        }
        return reloj;
    }

    // Sin synchronized en el metodo para permitir concurrencia real entre hilos
    public String procesarSolicitud(String lineaJson) {
        Map<String, String> msg = MensajeJson.parsear(lineaJson);
        String tipo = msg.getOrDefault("tipo", "");
        String token = msg.getOrDefault("token", "");
        int vid = Integer.parseInt(msg.getOrDefault("ventanilla", "-1"));

        // A3: Validacion de token
        if (token.isEmpty() || !tokenValido.equals(token)) {
            int l = registrarLog("RECHAZO_TOKEN", "Solicitud " + tipo + " rechazada desde V" + vid + " por token invalido");
            return MensajeJson.construir(
                    "tipo", "RESPUESTA",
                    "resultado", "RECHAZADA",
                    "motivo", "token invalido",
                    "lamport", l
            );
        }

        // B1 y B3: Asignacion de turnos (Normal vs -inseguro)
        if ("TURNO".equals(tipo)) {
            if (!modoInseguro) {
                synchronized (lockTurnos) {
                    if (emitidos >= cupo) {
                        int l = registrarLog("TURNO_RECHAZADO", "Cupo agotado (" + cupo + ") para V" + vid);
                        return MensajeJson.construir(
                                "tipo", "RESPUESTA",
                                "resultado", "RECHAZADA",
                                "motivo", "cupo agotado",
                                "emitidos", emitidos,
                                "cupo", cupo,
                                "lamport", l
                        );
                    }
                    emitidos++;
                    int numero = emitidos;
                    colaEspera.add(numero);
                    int l = registrarLog("TURNO_ASIGNADO", "Turno #" + numero + " entregado a V" + vid);
                    System.out.println("[COORDINADOR] Turno #" + numero + "/" + cupo + " asignado a V" + vid);
                    return MensajeJson.construir(
                            "tipo", "RESPUESTA",
                            "resultado", "ASIGNADO",
                            "numero", numero,
                            "emitidos", emitidos,
                            "cupo", cupo,
                            "lamport", l
                    );
                }
            } else {
                // B3: Lectura y actualizacion separadas por 50 ms sin exclusion mutua
                int actual = emitidos;
                if (actual >= cupo) {
                    int l = registrarLog("TURNO_RECHAZADO", "Cupo agotado (" + cupo + ") para V" + vid);
                    return MensajeJson.construir(
                            "tipo", "RESPUESTA",
                            "resultado", "RECHAZADA",
                            "motivo", "cupo agotado",
                            "emitidos", emitidos,
                            "cupo", cupo,
                            "lamport", l
                    );
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                emitidos = actual + 1;
                int numero = emitidos;
                synchronized (colaEspera) {
                    colaEspera.add(numero);
                }
                int l = registrarLog("TURNO_ASIGNADO_INSEGURO", "Turno #" + numero + " entregado a V" + vid);
                System.out.println("[COORDINADOR -INSEGURO] Turno #" + numero + "/" + cupo + " asignado a V" + vid);
                return MensajeJson.construir(
                        "tipo", "RESPUESTA",
                        "resultado", "ASIGNADO",
                        "numero", numero,
                        "emitidos", emitidos,
                        "cupo", cupo,
                        "lamport", l
                );
            }
        }

        // B1: LLAMAR entrega turnos en espera en orden FIFO a una sola ventanilla
        if ("LLAMAR".equals(tipo)) {
            synchronized (lockTurnos) {
                if (colaEspera.isEmpty()) {
                    int l = registrarLog("LLAMAR_RECHAZADO", "No hay turnos en espera para V" + vid);
                    return MensajeJson.construir(
                            "tipo", "RESPUESTA",
                            "resultado", "RECHAZADA",
                            "motivo", "no hay turnos en espera",
                            "lamport", l
                    );
                }
                int turnoLlamado = colaEspera.poll();
                atendidos++;
                int l = registrarLog("TURNO_LLAMADO", "Turno #" + turnoLlamado + " llamado por V" + vid);
                System.out.println("[COORDINADOR] V" + vid + " llama al turno #" + turnoLlamado);
                return MensajeJson.construir(
                        "tipo", "RESPUESTA",
                        "resultado", "LLAMADO",
                        "numero", turnoLlamado,
                        "emitidos", emitidos,
                        "cupo", cupo,
                        "lamport", l
                );
            }
        }

        if ("ESTADO".equals(tipo)) {
            synchronized (lockTurnos) {
                int l = registrarLog("ESTADO_CONSULTADO", "Estado enviado a V" + vid);
                return MensajeJson.construir(
                        "tipo", "RESPUESTA",
                        "resultado", "ESTADO",
                        "emitidos", emitidos,
                        "cupo", cupo,
                        "atendidos", atendidos,
                        "en_espera", new ArrayList<>(colaEspera),
                        "lamport", l
                );
            }
        }

        int l = registrarLog("SOLICITUD_INVALIDA", "Tipo desconocido: " + tipo);
        return MensajeJson.construir(
                "tipo", "RESPUESTA",
                "resultado", "RECHAZADA",
                "motivo", "tipo de solicitud no soportado",
                "lamport", l
        );
    }

    public static void main(String[] args) throws IOException {
        boolean inseguro = false;
        for (String arg : args) {
            if ("-inseguro".equalsIgnoreCase(arg)) {
                inseguro = true;
            }
        }

        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            props.load(fis);
        }

        int puerto = Integer.parseInt(props.getProperty("puerto", "5000").trim());
        int cupo = Integer.parseInt(props.getProperty("cupo", "10").trim());
        String token = props.getProperty("token", "").trim();

        Coordinador coord = new Coordinador(cupo, token, inseguro);
        ExecutorService pool = Executors.newCachedThreadPool();

        try (ServerSocket serverSocket = new ServerSocket(puerto)) {
            System.out.println("[COORDINADOR 0] Escuchando en puerto " + puerto + "...");
            while (true) {
                Socket cliente = serverSocket.accept();
                pool.submit(() -> manejarConexion(cliente, coord));
            }
        }
    }

    private static void manejarConexion(Socket socket, Coordinador coord) {
        try (
                Socket s = socket;
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)
        ) {
            String linea;
            while ((linea = in.readLine()) != null) {
                if (!linea.isBlank()) {
                    out.println(coord.procesarSolicitud(linea));
                }
            }
        } catch (IOException ignored) {
        }
    }
}