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

        try (Socket orchestratorSocket = new Socket(orchestratorHost, orchestratorPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(orchestratorSocket.getInputStream()));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(orchestratorSocket.getOutputStream()), true)) {

            System.out.println("[Worker] Conectado ao Orquestrador na porta " + orchestratorPort);

            String command;

            // Loop principal: Fica aguardando comandos do Orquestrador
            while ((command = in.readLine()) != null) {

                if (command.startsWith("PROCESS ")) {
                    String urlToProcess = command.substring(8).trim();

                    // Delega a tarefa para uma thread do Pool
                    ProcessUrlTask task = new ProcessUrlTask(urlToProcess, dataServerHost, dataServerPort, out);
                    threadPool.submit(task);
                }
            }
        } catch (IOException e) {
            System.err.println("[Worker] Conexão com o Orquestrador perdida/encerrada: " + e.getMessage());
        } finally {
            threadPool.shutdown();
            System.out.println("[Worker] Pool de threads encerrado.");
        }
    }

}
