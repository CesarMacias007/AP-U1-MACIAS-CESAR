# AP-U1-MACIAS-CESAR: TurnoJusto (Turnos de Atención Distribuidos)

**Aporte Práctico de la Unidad I -- Aplicaciones Distribuidas (ISR-701)**  
**Estudiante:** César Abrahan Macias Vásquez (`cmaciasv4@uteq.edu.ec`)  
**Docente:** Prof. Gleiston C. Guerrero-Ulloa, PhD  
**Periodo Académico:** SPA 2026--2027  

---

## 1. Instrucciones de Compilación y Ejecución del Código (Java 21)

El sistema opera completamente con la biblioteca estándar de Java 21 (sockets TCP, concurrencia, streams y colecciones), sin requerir dependencias externas ni gestores de paquetes.

### Compilación de clases
Desde la raíz del repositorio:
```bash
javac -encoding UTF-8 -d out src/*.java
```

### Orden de arranque de los procesos

1. **Coordinador Central (ID 0)**:
   - Modo Normal (con exclusión mutua):
     ```bash
     java -cp out Coordinador
     ```
   - Modo Inseguro (con pausa artificial de 50 ms para pruebas de condición de carrera):
     ```bash
     java -cp out Coordinador -inseguro
     ```

2. **Ventanillas (V1, V2, V3)**:
   Abrir terminales independientes para cada ventanilla:
   ```bash
   # Terminal 1 - Ventanilla 1
   java -cp out Ventanilla 1

   # Terminal 2 - Ventanilla 2
   java -cp out Ventanilla 2

   # Terminal 3 - Ventanilla 3
   java -cp out Ventanilla 3
   ```
   **Órdenes disponibles en la consola de cada ventanilla:**
   - `turno`: solicita un nuevo número de turno consecutivo para un cliente.
   - `llamar`: solicita al coordinador el siguiente turno en espera (atención FIFO).
   - `estado`: consulta cupo, turnos emitidos, atendidos, en espera y ventanillas activas/caídas.
   - `token_malo`: envía una petición con token erróneo para probar rechazo y bitácora.
   - `salir`: finaliza la ventanilla (provoca alerta de caída en coordinador tras 6 segundos).

3. **Prueba de Carga Concurrente (30 solicitudes simultáneas)**:
   ```bash
   java -cp out PruebaCarga
   ```

4. **Generación del Orden Total de Eventos (Lamport)**:
   ```bash
   java -cp out OrdenTotal
   ```
   Genera el archivo unificado y ordenado `logs/orden_total.log`.

---

## 2. Formato de Mensajes

La comunicación entre ventanillas y coordinador se realiza sobre sockets TCP mediante tramas JSON delimitadas por salto de línea (`\n`), evitando traslapes en ráfagas consecutivas:

- **Solicitud de Turno:**
  `{"tipo":"TURNO","token":"UTEQ2026_SECRETO","ventanilla":1,"lamport":12}`
- **Respuesta de Turno Asignado:**
  `{"tipo":"RESPUESTA","resultado":"ASIGNADO","numero":1,"emitidos":1,"cupo":10,"lamport":14}`
- **Solicitud de Llamar Turno:**
  `{"tipo":"LLAMAR","token":"UTEQ2026_SECRETO","ventanilla":2,"lamport":25}`
- **Latido Periódico (cada 2 segundos):**
  `{"tipo":"LATIDO","token":"UTEQ2026_SECRETO","ventanilla":3,"lamport":31}`

---

## 3. Compilación de los Documentos en LaTeX

Para compilar los documentos a PDF sin conexión a Internet se utiliza `pdflatex`:

### Compilación del Informe Técnico (`docs/informe.pdf`):
```bash
cd docs
pdflatex -interaction=nonstopmode informe.tex
pdflatex -interaction=nonstopmode informe.tex
```
*(Se ejecuta dos veces para resolver las referencias cruzadas de figuras y cuadros).*

### Compilación de la Carátula para el SGA (`docs/entrega_SGA.pdf`):
```bash
cd docs
pdflatex -interaction=nonstopmode entrega_SGA.tex
```

---

## 4. Declaración de Código Reutilizado

No se reutilizó código. Todo el código fue desarrollado de forma individual para la evaluación.



<img width="1347" height="641" alt="image" src="https://github.com/user-attachments/assets/409fadba-b5a0-4f60-96cc-295e308383b6" />

