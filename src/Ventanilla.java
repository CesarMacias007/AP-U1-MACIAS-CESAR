import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class Ventanilla {
    private final int id;
    private final String host;
    private final int puerto;
    private final String token;
    private int reloj = 0;

    public Ventanilla(int id, String host, int puerto, String token) {
        this.id = id;
        this.host = host;
        this.puerto = puerto;
        this.token = token;
    }

    public Map<String, String> enviarSolicitud(String tipo, String tokenEnviar) throws IOException {
        reloj++;
        String json = MensajeJson.construir(
                "tipo", tipo,
                "token", tokenEnviar,
                "ventanilla", id,
                "lamport", reloj
        );

        // Delimitacion por linea (\n) mediante println y readLine (Requerimiento A2)
        try (
                Socket socket = new Socket(host, puerto);
                PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
        ) {
            out.println(json);
            String respuesta = in.readLine();
            if (respuesta == null) {
                throw new IOException("Sin respuesta del coordinador");
            }
            return MensajeJson.parsear(respuesta);
        }
    }

    public void iniciarConsola() {
        System.out.println("=== VENTANILLA V" + id + " (Parte A: Comunicacion y Token) ===");
        System.out.println("Ordenes: turno | llamar | estado | token_malo | salir");

        try (Scanner sc = new Scanner(System.in, StandardCharsets.UTF_8)) {
            while (true) {
                System.out.print("V" + id + "> ");
                if (!sc.hasNextLine()) break;
                String orden = sc.nextLine().trim().toLowerCase();

                if (orden.isEmpty()) continue;
                if ("salir".equals(orden)) break;

                try {
                    switch (orden) {
                        case "turno" -> {
                            Map<String, String> r = enviarSolicitud("TURNO", token);
                            if ("ASIGNADO".equals(r.get("resultado"))) {
                                System.out.println("-> ASIGNADO turno #" + r.get("numero")
                                        + " (" + r.get("emitidos") + "/" + r.get("cupo") + ")");
                            } else {
                                System.out.println("-> RECHAZADA: " + r.get("motivo"));
                            }
                        }
                        case "llamar" -> {
                            Map<String, String> r = enviarSolicitud("LLAMAR", token);
                            if ("LLAMADO".equals(r.get("resultado"))) {
                                System.out.println("-> LLAMANDO al turno #" + r.get("numero"));
                            } else {
                                System.out.println("-> RECHAZADA: " + r.get("motivo"));
                            }
                        }
                        case "estado" -> {
                            Map<String, String> r = enviarSolicitud("ESTADO", token);
                            System.out.println("-> ESTADO: Emitidos=" + r.get("emitidos") + "/" + r.get("cupo")
                                    + ", Atendidos=" + r.get("atendidos")
                                    + ", En espera=" + r.get("en_espera"));
                        }
                        case "token_malo" -> {
                            Map<String, String> r = enviarSolicitud("TURNO", "TOKEN_INCORRECTO");
                            System.out.println("-> Respuesta token invalido: "
                                    + r.get("resultado") + " (" + r.get("motivo") + ")");
                        }
                        default -> System.out.println("Orden no valida. Use: turno, llamar, estado, token_malo, salir");
                    }
                } catch (IOException e) {
                    System.out.println("-> Error de conexion con el coordinador: " + e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || (!args[0].equals("1") && !args[0].equals("2") && !args[0].equals("3"))) {
            System.out.println("Uso: java -cp out Ventanilla <1|2|3>");
            System.exit(1);
        }

        int vid = Integer.parseInt(args[0]);
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            props.load(fis);
        }

        String host = props.getProperty("host", "127.0.0.1").trim();
        int puerto = Integer.parseInt(props.getProperty("puerto", "5000").trim());
        String token = props.getProperty("token", "").trim();

        new Ventanilla(vid, host, puerto, token).iniciarConsola();
    }
}