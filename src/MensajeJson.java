import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MensajeJson {

    public static Map<String, String> parsear(String json) {
        Map<String, String> mapa = new HashMap<>();
        if (json == null) return mapa;
        String limpio = json.trim();
        if (limpio.startsWith("{")) limpio = limpio.substring(1);
        if (limpio.endsWith("}")) limpio = limpio.substring(0, limpio.length() - 1);

        for (String par : dividir(limpio, ',')) {
            List<String> kv = dividir(par, ':');
            if (kv.size() == 2) {
                String clave = quitarComillas(kv.get(0).trim());
                String valor = quitarComillas(kv.get(1).trim());
                mapa.put(clave, valor);
            }
        }
        return mapa;
    }

    public static String construir(Object... pares) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < pares.length; i += 2) {
            if (i > 0) sb.append(", ");
            String clave = String.valueOf(pares[i]);
            Object valor = pares[i + 1];
            sb.append("\"").append(clave).append("\": ");
            if (valor instanceof Number || valor instanceof Boolean || valor instanceof List<?>) {
                sb.append(valor);
            } else {
                sb.append("\"").append(String.valueOf(valor).replace("\"", "\\\"")).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static List<String> dividir(String texto, char sep) {
        List<String> partes = new ArrayList<>();
        StringBuilder actual = new StringBuilder();
        boolean enComillas = false;
        int corchetes = 0;

        for (int i = 0; i < texto.length(); i++) {
            char c = texto.charAt(i);
            if (c == '"' && (i == 0 || texto.charAt(i - 1) != '\\')) {
                enComillas = !enComillas;
            } else if (!enComillas) {
                if (c == '[') corchetes++;
                else if (c == ']') corchetes--;
            }
            if (c == sep && !enComillas && corchetes == 0) {
                partes.add(actual.toString());
                actual.setLength(0);
            } else {
                actual.append(c);
            }
        }
        if (!actual.isEmpty()) partes.add(actual.toString());
        return partes;
    }

    private static String quitarComillas(String s) {
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }
}