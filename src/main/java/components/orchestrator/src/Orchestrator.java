package components.orchestrator.src;

import utils.Color;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Orchestrator {

    private int port;

    private final BlockingQueue<String> urlQueue = new LinkedBlockingQueue<>();
    private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet();

    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private final AtomicInteger successfulPages = new AtomicInteger(0);

    private volatile boolean isRunning = true;
    private ServerSocket serverSocket;
    private long startTime;

    private final ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();

    public Orchestrator(int port) {
        this.port = port;
        this.run();
    }

    private void initSeeds() {
        // Adicionar URLs iniciais (seeds) à fila
        addUrl("google.com");
        addUrl("wikipedia.org");
        addUrl("github.com");
        addUrl("stackoverflow.com");
    }

    private void addUrl(String url) {
        if (visitedUrls.add(url)) {
            urlQueue.offer(url);
        }
    }

    public void incrementSuccessCount() {
        successfulPages.incrementAndGet();
    }

    public void run() {
        System.out.println(Color.infoMessage("[ORCH] Coordenador iniciado na porta " + port));
        initSeeds();
        this.startTime = System.currentTimeMillis();

        try {
            serverSocket = new ServerSocket(port);

            while (isRunning) {
                try {
                    Socket workerSocket = serverSocket.accept(); // Bloqueia aqui até chegar conexão
                    System.out.println(Color.successMessage("[ORCH] Novo Worker conectado: " + workerSocket.getInetAddress()));

                    WorkerHandler handler = new WorkerHandler(
                            workerSocket, urlQueue, visitedUrls, activeTasks, this
                    );
                    workerPool.submit(handler);

                } catch (SocketException e) {
                    if (!isRunning) {
                        System.out.println(Color.errorMessage("[ORCH] ServerSocket destravado para encerramento."));
                        break; // Sai do loop principal
                    }
                    throw e;
                }
            }
        } catch (IOException e) {
            System.err.println(Color.errorMessage("[ORCH] Erro no servidor: " + e.getMessage()));
        } finally {
            executeShutdownRoutine();
        }
    }

    // Método que os Workers chamam para tentar encerrar o sistema
    public synchronized void tryShutdown() {
        if (isRunning && urlQueue.isEmpty() && activeTasks.get() == 0) {
            System.out.println(Color.successMessage("\n[ORCH] Critério de parada atingido! Fila vazia e Workers ociosos."));
            isRunning = false;
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close(); // Força a saída do accept() bloqueante
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void executeShutdownRoutine() {

        long endTime = System.currentTimeMillis();
        long durationMs = endTime - this.startTime;
        double durationSec = durationMs / 1000.0;

        System.out.println(Color.header("\n--- Iniciando desligamento do Orquestrador ---"));
        workerPool.shutdownNow();

        System.out.println(Color.highlight("\n==========================================================================="));
        System.out.println(Color.highlight("[ORCH] Total de URLs descobertas e tentadas: " + visitedUrls.size()));
        System.out.println(Color.highlight("[ORCH] Total de páginas válidas indexadas: " + successfulPages.get()));
        System.out.println(Color.highlight("[ORCH] Total de páginas quebradas (404): " + (visitedUrls.size() - successfulPages.get())));
        System.out.println(Color.highlight("[ORCH] Tempo total de processamento: ") + durationSec + " segundos (" + durationMs + " ms).");
        System.out.println(Color.highlight("===========================================================================\n"));

        // TODO: analise do processamento

        System.out.println(Color.successMessage("[ORCH] Processo finalizado com sucesso."));
    }

    public boolean isRunning() {
        return isRunning;
    }

}
