package components.worker.src;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class ProcessUrlTask implements Runnable{

    private final String url;
    private final String dataServerHost;
    private final int dataServerPort;
    private final PrintWriter orchestratorOut;

    public ProcessUrlTask(String url, String dataServerHost, int dataServerPort, PrintWriter orchestratorOut) {
        this.url = url;
        this.dataServerHost = dataServerHost;
        this.dataServerPort = dataServerPort;
        this.orchestratorOut = orchestratorOut;
    }

    @Override
    public void run() {
        System.out.println("[WorkerTask] Iniciando processamento da URL: " + url);

        try (Socket dataServerSocket = new Socket(dataServerHost, dataServerPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(dataServerSocket.getInputStream()));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(dataServerSocket.getOutputStream()), true)) {

            out.println("GET /" + url + " HTTP/1.1");

            String response = in.readLine();

            if (response != null && response.startsWith("LINKS: ")) {

                simulateCpuBoundWork();

                String linksRaw = response.substring(7);

                List<String> validLinks = validateLinks(linksRaw);

                String linksJoined = String.join(", ", validLinks);
                String resultMessage = "FOUND: " + linksJoined + " FROM " + url;

                // O bloco synchronized garante que duas threads não embaralhem as mensagens no mesmo socket
                synchronized (orchestratorOut) {
                    orchestratorOut.println(resultMessage);
                }

                System.out.println("[WorkerTask] Sucesso: " + url + " -> " + validLinks.size() + " link(s) validados.");
            } else {
                System.out.println("[WorkerTask] Beco sem saída ou erro na URL: " + url);
                synchronized (orchestratorOut) {
                    orchestratorOut.println("FOUND:  FROM " + url); // Retorna vazio para a fila andar
                }
            }
        } catch (IOException e) {
            System.err.println("[WorkerTask] Erro ao conectar com DataServer para URL " + url + ": " + e.getMessage());
        }
    }

    private void simulateCpuBoundWork() {
        try {
            // Gera um número aleatório entre 2000 (inclusivo) e 5001 (exclusivo)
            long sleepTime = ThreadLocalRandom.current().nextLong(2000, 5001);
            System.out.println("[WorkerTask] Processando conteudo da página");

            Thread.sleep(sleepTime);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[WorkerTask] Processamento CPU Bound interrompido para " + url);
        }
    }

    private List<String> validateLinks(String linksRaw) {
        if (linksRaw.isBlank()) {
            return List.of();
        }

        return Arrays.stream(linksRaw.split(","))
                .map(String::trim)
                .filter(link -> !link.isBlank())
                // Validação de Integridade: Remove links que apontam para a própria página (auto-referência)
                .filter(link -> !link.equalsIgnoreCase(this.url))
                .collect(Collectors.toList());
    }
}
