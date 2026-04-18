package components.dataserver.src;

import utils.Ports;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class WorkerHandlerDataserver {

    private final String serverHost;
    private final int serverPort;
    private final ExecutorService executorService;

    public WorkerHandlerDataserver(String serverHost, int serverPort, int numThreads) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.executorService = Executors.newFixedThreadPool(numThreads);
    }

    public void processUrls(List<String> urls) {
        System.out.println("[Worker] Iniciando processamento paralelo de " + urls.size() + " URLs...");

        for (String url : urls) {
            executorService.submit(() -> fetchSiteData(url));
        }
    }

    private void fetchSiteData(String url) {
        try (
                Socket socket = new Socket(serverHost, serverPort);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            // GET /<url> HTTP/1.1
            String request = "GET /" + url + " HTTP/1.1";
            out.println(request);

            String response = in.readLine();

            System.out.println("[Worker] URL: " + url + " | Resposta do Servidor: " + response);

        } catch (Exception e) {
            System.err.println("[Worker] Erro ao conectar/processar a URL " + url + ": " + e.getMessage());
        }
    }

    public void shutdown() {
        System.out.println("[Worker] Encerrando o pool de threads...");
        executorService.shutdown();
        try {
            // Aguarda até 60 segundos para que as tarefas em andamento terminem
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow(); // Força o encerramento se demorar muito
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("[Worker] Pool de threads encerrado.");
    }

    public static void main(String[] args) {
        WorkerHandlerDataserver worker = new WorkerHandlerDataserver("localhost", Ports.DATASERVER, 5);

        List<String> urlsToProcess = List.of(
                "google.com",
                "wikipedia.com",
                "medium.com",
                "reddit.com"
        );

        worker.processUrls(urlsToProcess);

        worker.shutdown();
    }
}