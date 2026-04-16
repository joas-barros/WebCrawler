package components.orchestrator.src;

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

    private volatile boolean isRunning = true;
    private ServerSocket serverSocket;

    private final ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();

    public Orchestrator(int port) {
        this.port = port;
        this.run();
    }

    private void initSeeds() {
        // Adicionar URLs iniciais (seeds) à fila
        addUrl("http://example.com");
        addUrl("http://example.org");
        addUrl("http://example.net");
    }

    private void addUrl(String url) {
        if (visitedUrls.add(url)) {
            urlQueue.offer(url);
        }
    }

    public void run() {
        System.out.println("Coordenador iniciado na porta " + port);
        initSeeds();

        try {
            serverSocket = new ServerSocket(port);

            while (isRunning) {
                try {
                    Socket workerSocket = serverSocket.accept(); // Bloqueia aqui até chegar conexão
                    System.out.println("Novo Worker conectado: " + workerSocket.getInetAddress());

                    WorkerHandler handler = new WorkerHandler(
                            workerSocket, urlQueue, visitedUrls, activeTasks, this
                    );
                    workerPool.submit(handler);

                } catch (SocketException e) {
                    if (!isRunning) {
                        System.out.println("ServerSocket destravado para encerramento.");
                        break; // Sai do loop principal
                    }
                    throw e;
                }
            }
        } catch (IOException e) {
            System.err.println("Erro no servidor: " + e.getMessage());
        } finally {
            executeShutdownRoutine();
        }
    }

    // Método que os Workers chamam para tentar encerrar o sistema
    public synchronized void tryShutdown() {
        if (isRunning && urlQueue.isEmpty() && activeTasks.get() == 0) {
            System.out.println("\nCritério de parada atingido! Fila vazia e Workers ociosos.");
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
        System.out.println("\n--- Iniciando desligamento do Orquestrador ---");
        workerPool.shutdownNow(); // Interrompe todas as threads dos Workers

        System.out.println("Total de URLs únicas processadas: " + visitedUrls.size());

        // TODO: analise do processamento

        System.out.println("Processo finalizado com sucesso.");
    }

    public boolean isRunning() {
        return isRunning;
    }

}
