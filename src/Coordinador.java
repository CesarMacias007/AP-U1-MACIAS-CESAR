import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Coordinador {
    private static final long TIMEOUT_CAIDA_MS = 6000L;

    private final int cupo;
    private final String tokenValido;
    private final boolean modoInseguro;
    private final RelojLamport reloj;

    private final Object lockTurnos = new Object();
    private final Object lockLatidos = new Object();

    private int emitidos = 0;
    private int atendidos = 0;
    private final Queue<Integer> colaEspera = new LinkedList<>();

    private final Map<Integer, Long> ultimoLatido = new ConcurrentHashMap<>();
    private final Map<Integer, String> estadoVentanillas = new ConcurrentHashMap<>();

    public Coordinador(int cupo, String tokenValido, boolean modoInseguro) {
        this.cupo = cupo;
        this.tokenValido = tokenValido;
        this.modoInseguro = modoInseguro;
        this.reloj = new RelojLamport(0, "coordinador");

        for (int v : new int[]{1, 2, 3}) {
            estadoVentanillas.put(v, "SIN_CONEXION");
        }

        String modo = modoInseguro ? "INSEGURO (pausa 50 ms)" : "NORMAL (con exclusion mutua)";
        reloj.eventoLocal("INICIO_COORDINADOR", "Cupo=" + cupo + ", Modo=" + modo);
        System.out.println("[COORDINADOR 0] Iniciado | Cupo=" + cupo + " | Modo=" + modo);
    }

    public void iniciarMonitorLatidos() {
        Thread monitor = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                long ahora = System.currentTimeMillis();
                synchronized (lockLatidos) {
                    for (int vid : new int[]{1, 2, 3}) {
                        Long ultimo = ultimoLatido.get(vid);
                        if (ultimo != null && (ahora - ultimo) > TIMEOUT_CAIDA_MS) {
                            if ("ACTIVA".equals(estadoVentanillas.get(vid))) {
                                estadoVentanillas.put(vid, "CAIDA");
                                int l = reloj.eventoLocal(
                                        "VENTANILLA_CAIDA",
                                        "Ventanilla V" + vid + " sin latidos durante >6s"
                                );
                                System.out.println("[COORDINADOR | L=" + l + "] ALERTA: Ventanilla V" + vid + " CAIDA");
                            }
                        }
                    }
                }
            }
        });
        monitor.setDaemon(true);
        monitor.start();
    }

    public String procesarSolicitud(String lineaJson) {
        Map<String, String> msg = MensajeJson.parsear(lineaJson);
        String tipo = msg.getOrDefault("tipo", "");
        String token = msg.getOrDefault("token", "");
        int vid = Integer.parseInt(msg.getOrDefault("ventanilla", "-1"));
        int lamportRemoto = Integer.parseInt(msg.getOrDefault("lamport", "0"));

        // D1: Actualizar reloj de Lamport al recibir mensaje
        reloj.recibir(lamportRemoto, "RECIBIR_" + tipo, "Mensaje recibido desde V" + vid);

        // A3: Validacion de token
        if (token.isEmpty() || !tokenValido.equals(token)) {
            int l = reloj.enviar("RECHAZO_TOKEN", "Solicitud " + tipo + " rechazada desde V" + vid + " por token invalido");
            System.out.println("[COORDINADOR | L=" + l + "] RECHAZO: Token invalido desde V" + vid);
            return MensajeJson.construir(
                    "tipo", "RESPUESTA",
                    "resultado", "RECHAZADA",
                    "motivo", "token invalido",
                    "lamport", l
            );
        }

        // C1-C2: Latidos
        if ("LATIDO".equals(tipo)) {
            synchronized (lockLatidos) {
                ultimoLatido.put(vid, System.currentTimeMillis());
                String estadoAnterior = estadoVentanillas.getOrDefault(vid, "SIN_CONEXION");
                if (!"ACTIVA".equals(estadoAnterior)) {
                    estadoVentanillas.put(vid, "ACTIVA");
                    int lAct = reloj.eventoLocal(
                            "VENTANILLA_ACTIVA",
                            "Ventanilla V" + vid + " paso de " + estadoAnterior + " a ACTIVA"
                    );
                    System.out.println("[COORDINADOR | L=" + lAct + "] INFO: Ventanilla V" + vid + " ACTIVA");
                }
            }
            int lResp = reloj.enviar("ACK_LATIDO", "Confirmacion de latido enviada a V" + vid);
            return MensajeJson.construir(
                    "tipo", "RESPUESTA",
                    "resultado", "OK",
                    "lamport", lResp
            );
        }

        // B1-B3: Asignacion de turnos
        if ("TURNO".equals(tipo)) {
            if (!modoInseguro) {
                synchronized (lockTurnos) {
                    if (emitidos >= cupo) {
                        int l = reloj.enviar("TURNO_RECHAZADO", "Cupo agotado (" + cupo + ") para V" + vid);
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
                    int l = reloj.enviar("TURNO_ASIGNADO", "Turno #" + numero + " asignado a V" + vid);
                    System.out.println("[COORDINADOR | L=" + l + "] Turno #" + numero + "/" + cupo + " -> V" + vid);
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
                int actual = emitidos;
                if (actual >= cupo) {
                    int l = reloj.enviar("TURNO_RECHAZADO", "Cupo agotado (" + cupo + ") para V" + vid);
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
                int l = reloj.enviar("TURNO_ASIGNADO_INSEGURO", "Turno #" + numero + " asignado a V" + vid);
                System.out.println("[COORDINADOR -INSEGURO | L=" + l + "] Turno #" + numero + "/" + cupo + " -> V" + vid);
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

        // B1: Llamar siguiente turno
        if ("LLAMAR".equals(tipo)) {
            synchronized (lockTurnos) {
                if (colaEspera.isEmpty()) {
                    int l = reloj.enviar("LLAMAR_RECHAZADO", "No hay turnos en espera para V" + vid);
                    return MensajeJson.construir(
                            "tipo", "RESPUESTA",
                            "resultado", "RECHAZADA",
                            "motivo", "no hay turnos en espera",
                            "lamport", l
                    );
                }
                int turnoLlamado = colaEspera.poll();
                atendidos++;
                int l = reloj.enviar("TURNO_LLAMADO", "Turno #" + turnoLlamado + " llamado por V" + vid);
                System.out.println("[COORDINADOR | L=" + l + "] V" + vid + " llama al turno #" + turnoLlamado);
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

        // C3: Estado
        if ("ESTADO".equals(tipo)) {
            synchronized (lockTurnos) {
                synchronized (lockLatidos) {
                    List<Integer> activas = new ArrayList<>();
                    List<Integer> caidas = new ArrayList<>();
                    for (int v : new int[]{1, 2, 3}) {
                        String est = estadoVentanillas.get(v);
                        if ("ACTIVA".equals(est)) {
                            activas.add(v);
                        } else if ("CAIDA".equals(est)) {
                            caidas.add(v);
                        }
                    }
                    int l = reloj.enviar("ESTADO_ENVIADO", "Estado enviado a V" + vid);
                    return MensajeJson.construir(
                            "tipo", "RESPUESTA",
                            "resultado", "ESTADO",
                            "emitidos", emitidos,
                            "cupo", cupo,
                            "atendidos", atendidos,
                            "en_espera", new ArrayList<>(colaEspera),
                            "activas", activas,
                            "caidas", caidas,
                            "lamport", l
                    );
                }
            }
        }

        int l = reloj.enviar("SOLICITUD_INVALIDA", "Tipo desconocido: " + tipo);
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
        coord.iniciarMonitorLatidos();

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