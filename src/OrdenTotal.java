import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class OrdenTotal {

    record EventoLog(int lamport, int idProceso, String evento, String detalle) {
        @Override
        public String toString() {
            return lamport + ";" + idProceso + ";" + evento + ";" + detalle;
        }
    }

    public static void main(String[] args) throws IOException {
        Path carpetaLogs = Paths.get("logs");
        if (!Files.exists(carpetaLogs)) {
            System.out.println("No se encontro la carpeta logs/");
            return;
        }

        List<EventoLog> todos = new ArrayList<>();

        try (Stream<Path> flujo = Files.list(carpetaLogs)) {
            List<Path> archivosLog = flujo
                    .filter(p -> p.toString().endsWith(".log"))
                    .filter(p -> !p.getFileName().toString().equals("orden_total.log"))
                    .toList();

            for (Path archivo : archivosLog) {
                List<String> lineas = Files.readAllLines(archivo, StandardCharsets.UTF_8);
                for (String linea : lineas) {
                    if (linea.isBlank()) continue;
                    String[] partes = linea.split(";", 4);
                    if (partes.length == 4) {
                        int lamport = Integer.parseInt(partes[0].trim());
                        int idProc = Integer.parseInt(partes[1].trim());
                        String evento = partes[2].trim();
                        String detalle = partes[3].trim();
                        todos.add(new EventoLog(lamport, idProc, evento, detalle));
                    }
                }
            }
        }

        // D3: Ordenar por marca de Lamport y desempatar por id_proceso
        todos.sort(
                Comparator.comparingInt(EventoLog::lamport)
                        .thenComparingInt(EventoLog::idProceso)
        );

        List<String> lineasOrdenadas = todos.stream()
                .map(EventoLog::toString)
                .toList();

        Path salida = carpetaLogs.resolve("orden_total.log");
        Files.write(salida, lineasOrdenadas, StandardCharsets.UTF_8);

        System.out.println("Archivo generado: " + salida + " (" + lineasOrdenadas.size() + " eventos ordenados).");
    }
}