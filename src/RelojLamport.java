import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class RelojLamport {
    private final int idProceso;
    private final Path rutaLog;
    private int reloj;

    public RelojLamport(int idProceso, String nombreArchivo) {
        this.idProceso = idProceso;
        this.reloj = 0;
        try {
            Files.createDirectories(Paths.get("logs"));
        } catch (IOException e) {
            System.err.println("Error creando carpeta logs: " + e.getMessage());
        }
        this.rutaLog = Paths.get("logs", nombreArchivo + ".log");
    }

    // D1: Incrementa en 1 antes de un evento local
    public synchronized int eventoLocal(String evento, String detalle) {
        reloj++;
        escribir(reloj, evento, detalle);
        return reloj;
    }

    // D1: Incrementa en 1 antes de enviar un mensaje
    public synchronized int enviar(String evento, String detalle) {
        reloj++;
        escribir(reloj, evento, detalle);
        return reloj;
    }

    // D1: Al recibir un mensaje, fija el reloj en max(local, recibido) + 1
    public synchronized int recibir(int relojRemoto, String evento, String detalle) {
        reloj = Math.max(reloj, relojRemoto) + 1;
        escribir(reloj, evento, detalle);
        return reloj;
    }

    public synchronized int getValor() {
        return reloj;
    }

    // D2: Formato lamport;id_proceso;evento;detalle
    private void escribir(int lamport, String evento, String detalle) {
        String detalleLimpio = detalle.replace(";", ",");
        String linea = lamport + ";" + idProceso + ";" + evento + ";" + detalleLimpio + System.lineSeparator();
        try {
            Files.writeString(
                    rutaLog,
                    linea,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            System.err.println("Error escribiendo en " + rutaLog + ": " + e.getMessage());
        }
    }
}