package components.worker.src;

import utils.Color;
import utils.Labels;

import java.io.*;
import java.net.Socket;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Worker {

    private final String orchestratorHost;
    private final int orchestratorPort;
    private final String dataServerHost;
    private final int dataServerPort;

    private final int capacity;
    private final String identification;

    private final ExecutorService threadPool;

    private final AtomicInteger totalProcessedTasks = new AtomicInteger(0);

    private final Map<String, Labels> processedUrlsReport = new ConcurrentHashMap<>();

    private static final int RETRY_DELAY_MS = 5000;

    public Worker(String orchestratorHost, int orchestratorPort, String dataServerHost, int dataServerPort, int capacity, String identification) {
        this.orchestratorHost = orchestratorHost;
        this.orchestratorPort = orchestratorPort;
        this.dataServerHost = dataServerHost;
        this.dataServerPort = dataServerPort;
        this.capacity = capacity;
        this.identification = identification;
        this.threadPool = Executors.newFixedThreadPool(capacity);

        run();
    }

    public void run() {
        System.out.println(Color.header("[Worker] Iniciando com capacidade para " + capacity + " tarefas simultâneas."));

        while (true) {
            try (Socket orchestratorSocket = new Socket(orchestratorHost, orchestratorPort);
                 BufferedReader in = new BufferedReader(new InputStreamReader(orchestratorSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(orchestratorSocket.getOutputStream()), true)) {

                System.out.println(Color.successMessage("[Worker] Sucesso! Conectado ao Orquestrador na porta ") + orchestratorPort);

                String command;

                while ((command = in.readLine()) != null) {
                    if (command.startsWith("PROCESS ")) {
                        String urlToProcess = command.substring(8).trim();

                        ProcessUrlTask task = new ProcessUrlTask(urlToProcess, dataServerHost, dataServerPort, out, identification, totalProcessedTasks, processedUrlsReport);
                        threadPool.submit(task);
                    } else {
                        System.out.println(Color.warningMessage("[Worker] Comando desconhecido recebido: " + command));
                    }
                }

                System.out.println("[Worker] O Orquestrador encerrou a conexão de forma limpa. Finalizando o trabalho...");
                break;

            } catch (IOException e) {
                // Cai aqui se o Orquestrador não estiver online ou se a conexão cair no meio do caminho
                System.out.println(Color.errorMessage("[Worker] Falha ao comunicar com o Orquestrador: " + e.getMessage()));
                System.out.println(Color.warningMessage("[Worker] Tentando reconectar em " + (RETRY_DELAY_MS / 1000) + " segundos..."));

                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.out.println(Color.errorMessage("[Worker] Processo de retry interrompido."));
                    break;
                }
            }
        }

        // Proíbe novas tarefas
        threadPool.shutdown();

        try {
            // Aguarda até que as tarefas que já estavam rodando (no Thread.sleep) terminem.
            // O Worker vai esperar até 1 minuto para as threads finalizarem pacificamente.
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow(); // Se demorar mais que 1 minuto, força a parada
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println(Color.highlight("\n--- RELATÓRIO DO " + identification.toUpperCase() + " ---"));
        System.out.println(Color.highlight("Total de URLs processadas: ") + totalProcessedTasks.get());

        // Agrupa as URLs por label para exibição
        Map<Labels, List<String>> byLabel = processedUrlsReport.entrySet().stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())
                ));

        System.out.println(Color.highlight("\nURLs processadas por categoria:"));
        byLabel.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().name()))
                .forEach(entry -> {
                    System.out.println(Color.highlight("  [" + entry.getKey() + "] (" + entry.getValue().size() + " URL(s)):"));
                    entry.getValue().stream()
                            .sorted()
                            .forEach(u -> System.out.println("    - " + u));
                });

        System.out.println(Color.successMessage("[" + identification + "] Operação encerrada completamente.\n"));
    }

}