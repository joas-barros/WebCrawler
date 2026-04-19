package components.worker.src;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {

    private final String orchestratorHost;
    private final int orchestratorPort;
    private final String dataServerHost;
    private final int dataServerPort;

    private final int capacity;
    private final ExecutorService threadPool;

    private static final int RETRY_DELAY_MS = 5000;

    public Worker(String orchestratorHost, int orchestratorPort, String dataServerHost, int dataServerPort, int capacity) {
        this.orchestratorHost = orchestratorHost;
        this.orchestratorPort = orchestratorPort;
        this.dataServerHost = dataServerHost;
        this.dataServerPort = dataServerPort;
        this.capacity = capacity;
        this.threadPool = Executors.newFixedThreadPool(capacity);

        run();
    }

    public void run() {
        System.out.println("[Worker] Iniciando com capacidade para " + capacity + " tarefas simultâneas.");

        while (true) {
            try (Socket orchestratorSocket = new Socket(orchestratorHost, orchestratorPort);
                 BufferedReader in = new BufferedReader(new InputStreamReader(orchestratorSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(orchestratorSocket.getOutputStream()), true)) {

                System.out.println("[Worker] Sucesso! Conectado ao Orquestrador na porta " + orchestratorPort);

                String command;

                while ((command = in.readLine()) != null) {
                    if (command.startsWith("PROCESS ")) {
                        String urlToProcess = command.substring(8).trim();

                        ProcessUrlTask task = new ProcessUrlTask(urlToProcess, dataServerHost, dataServerPort, out);
                        threadPool.submit(task);
                    }
                }

                System.out.println("[Worker] O Orquestrador encerrou a conexão de forma limpa. Finalizando o trabalho...");
                break;

            } catch (IOException e) {
                // Cai aqui se o Orquestrador não estiver online ou se a conexão cair no meio do caminho
                System.err.println("[Worker] Falha ao comunicar com o Orquestrador: " + e.getMessage());
                System.out.println("[Worker] Tentando reconectar em " + (RETRY_DELAY_MS / 1000) + " segundos...");

                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.out.println("[Worker] Processo de retry interrompido.");
                    break;
                }
            }
        }

        threadPool.shutdown();
        System.out.println("[Worker] Operação encerrada completamente.");
    }

}
