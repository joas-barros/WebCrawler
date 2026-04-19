package components.worker.src;

import utils.Color;

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
    private final String identification;

    public ProcessUrlTask(String url, String dataServerHost, int dataServerPort, PrintWriter orchestratorOut, String identification) {
        this.url = url;
        this.dataServerHost = dataServerHost;
        this.dataServerPort = dataServerPort;
        this.orchestratorOut = orchestratorOut;
        this.identification = identification;
    }

    @Override
    public void run() {
        System.out.println(Color.infoMessage("[WorkerTask] Iniciando processamento da URL: " + url));

        try (Socket dataServerSocket = new Socket(dataServerHost, dataServerPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(dataServerSocket.getInputStream()));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(dataServerSocket.getOutputStream()), true)) {

            String outputMessage = "GET /" + identification + "/" + url + " HTTP/1.1";
            System.out.println(Color.infoMessage("[WorkerTask] Enviando requisição para DataServer: " + outputMessage));

            out.println(outputMessage);

            String response = in.readLine();

            if (response != null && response.startsWith("LINKS: ")) {

                simulateCpuBoundWork();

                String linksRaw = response.substring(7);

                List<String> validLinks = validateLinks(linksRaw);

                String linksJoined = String.join(", ", validLinks);
                String resultMessage = identification + " FOUND: " + linksJoined + " FROM " + url;

                // O bloco synchronized garante que duas threads não embaralhem as mensagens no mesmo socket
                synchronized (orchestratorOut) {
                    System.out.println(Color.infoMessage("[WorkerTask] Enviando resultado para Orquestrador: " + resultMessage));
                    orchestratorOut.println(resultMessage);
                }

                System.out.println(Color.successMessage("[WorkerTask] Sucesso: " + url + " -> " + validLinks.size() + " link(s) validados."));
            } else if (response != null && response.startsWith("NOT_FOUND")) {
                System.out.println(Color.errorMessage("[WorkerTask] Erro 404, URL não existe no banco: " + url));
                synchronized (orchestratorOut) {
                    // Manda um comando diferente avisando que falhou!
                    orchestratorOut.println(identification + " FAILED: " + url);
                }
            } else {
                System.out.println(Color.errorMessage("[WorkerTask] Resposta inesperada do DataServer para URL " + url + ": " + response));
            }
        } catch (IOException e) {
            System.out.println(Color.errorMessage("[WorkerTask] Erro ao conectar com DataServer para URL " + url + ": " + e.getMessage()));
        }
    }

    private void simulateCpuBoundWork() {
        try {
            // Gera um número aleatório entre 2000 (inclusivo) e 5001 (exclusivo)
            long sleepTime = ThreadLocalRandom.current().nextLong(2000, 5001);
            System.out.println(Color.infoMessage("[WorkerTask] Processando conteudo da página"));

            Thread.sleep(sleepTime);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println(Color.errorMessage("[WorkerTask] Processamento CPU Bound interrompido para " + url));
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
