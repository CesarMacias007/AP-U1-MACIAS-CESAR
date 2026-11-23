import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CyclicBarrier;

public class PruebaCarga {

    record RespuestaTurno(String resultado, int numero) {}

    private static Map<String, String> enviar(String host, int puerto, String json) throws IOException {
        try (
                Socket socket = new Socket(host, puerto);
                PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
        ) {
            out.println(json);
            String linea = in.readLine();
            return MensajeJson.parsear(linea);
        }
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            props.load(fis);
        }

        String host = props.getProperty("host", "127.0.0.1").trim();
        int puerto = Integer.parseInt(props.getProperty("puerto", "5000").trim());
        String token = props.getProperty("token", "").trim();

        int totalHilos = 30;
        CyclicBarrier barrera = new CyclicBarrier(totalHilos);
        List<RespuestaTurno> respuestas = Collections.synchronizedList(new ArrayList<>());
        List<Thread> hilos = new ArrayList<>();

        for (int vid : new int[]{1, 2, 3}) {
            for (int i = 0; i < 10; i++) {
                final int ventanillaId = vid;
                final int relojHilo = i + 1;

                Thread t = new Thread(() -> {
                    try {
                        String solicitud = MensajeJson.construir(
                                "tipo", "TURNO",
                                "token", token,
                                "ventanilla", ventanillaId,
                                "lamport", relojHilo
                        );
                        barrera.await();
                        Map<String, String> resp = enviar(host, puerto, solicitud);
                        String resultado = resp.getOrDefault("resultado", "RECHAZADA");
                        int numero = Integer.parseInt(resp.getOrDefault("numero", "0"));
                        respuestas.add(new RespuestaTurno(resultado, numero));
                    } catch (Exception e) {
                        System.err.println("Error en hilo V" + ventanillaId + ": " + e.getMessage());
                    }
                });

                hilos.add(t);
                t.start();
            }
        }

        for (Thread t : hilos) {
            t.join();
        }

        List<Integer> asignados = new ArrayList<>();
        int rechazadas = 0;

        for (RespuestaTurno r : respuestas) {
            if ("ASIGNADO".equals(r.resultado())) {
                asignados.add(r.numero());
            } else {
                rechazadas++;
            }
        }

        Collections.sort(asignados);
        Set<Integer> distintos = new TreeSet<>(asignados);

        String reqEstado = MensajeJson.construir(
                "tipo", "ESTADO",
                "token", token,
                "ventanilla", 1,
                "lamport", 999
        );
        Map<String, String> estadoCoord = enviar(host, puerto, reqEstado);

        System.out.println("=== RESULTADOS PRUEBA DE CARGA (30 SOLICITUDES SIMULTANEAS) ===");
        System.out.println("Solicitudes ASIGNADAS         : " + asignados.size());
        System.out.println("Solicitudes RECHAZADAS        : " + rechazadas);
        System.out.println("Numeros distintos entregados  : " + distintos.size() + " -> " + distintos);
        System.out.println("Lista completa asignados      : " + asignados);
        System.out.println("Turnos emitidos segun coord.  : " + estadoCoord.get("emitidos") + " (Cupo: " + estadoCoord.get("cupo") + ")");
    }
}