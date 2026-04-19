package components.orchestrator.src;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerHandler implements Runnable{

    private final Socket socket;
    private final BlockingQueue<String> urlQueue;
    private final Set<String> visitedUrls;
    private final AtomicInteger activeTasks;
    private final Orchestrator orchestrator;

    public WorkerHandler(Socket socket,
                         BlockingQueue<String> urlQueue,
                         Set<String> visitedUrls,
                         AtomicInteger activeTasks,
                         Orchestrator orchestrator) {
        this.socket = socket;
        this.urlQueue = urlQueue;
        this.visitedUrls = visitedUrls;
        this.activeTasks = activeTasks;
        this.orchestrator = orchestrator;
    }

    @Override
    public void run() {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            // 1. Inicia a Thread ESCRITORA (Despacha tarefas)
            Thread writerThread = Thread.ofVirtual().start(() -> {
                try {
                    while (orchestrator.isRunning() && !socket.isClosed()) {
                        String nextUrl = urlQueue.poll(500, TimeUnit.MILLISECONDS);

                        if (nextUrl != null) {
                            activeTasks.incrementAndGet(); // Marca que a tarefa foi enviada
                            out.println("PROCESS " + nextUrl);
                        }

                        orchestrator.tryShutdown();
                    }
                } catch (Exception e) {
                    System.out.println("[Orchestrator] Thread escritora do Worker finalizada.");
                }
            });

            // 2. A thread principal vira a LEITORA (Processa respostas)
            String response;
            // readLine() bloqueia até o Worker enviar uma resposta
            while (orchestrator.isRunning() && (response = in.readLine()) != null) {

                if (response.startsWith("FOUND:")) {
                    processWorkerResponse(response);
                    activeTasks.decrementAndGet(); // Marca que a tarefa foi concluída
                }

                orchestrator.tryShutdown();
            }

            // Se o loop de leitura quebrar (ex: worker desconectou), interrompemos a escrita
            writerThread.interrupt();
        } catch (IOException e) {
            if (orchestrator.isRunning()) {
                System.out.println("Worker desconectado inesperadamente.");
            }
        }
    }

    private void processWorkerResponse(String response) {
        String linksPart = response.substring(7).split(" FROM")[0];
        String[] foundLinks = linksPart.split(", ");

        System.out.println("Worker encontrou links: " + String.join(", ", foundLinks));

        for (String link : foundLinks) {
            link = link.trim();
            if (!link.isEmpty() && visitedUrls.add(link)) {
                System.out.println("Adicionando nova URL à fila: " + link);
                urlQueue.add(link);
            }
        }
    }
}
