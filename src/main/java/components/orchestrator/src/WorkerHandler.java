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
            while (orchestrator.isRunning()) {

                String nextUrl = urlQueue.poll(500, TimeUnit.MILLISECONDS);

                if (nextUrl != null) {
                    activeTasks.incrementAndGet();
                    out.println("PROCESS " + nextUrl);

                    String response = in.readLine();
                    if (response != null && response.startsWith("FOUND:")) {
                        processWorkerResponse(response);
                    }

                    activeTasks.decrementAndGet();
                }

                // Sempre avisa o Orquestrador para verificar se já acabou
                orchestrator.tryShutdown();
            }
        } catch (IOException | InterruptedException e) {
            if (orchestrator.isRunning()) {
                System.out.println("Worker desconectado inesperadamente.");
            }
        }
    }

    private void processWorkerResponse(String response) {
        String linksPart = response.substring(7).split(" FROM")[0];
        String[] foundLinks = linksPart.split(", ");

        for (String link : foundLinks) {
            link = link.trim();
            if (!link.isEmpty() && visitedUrls.add(link)) {
                urlQueue.add(link);
            }
        }
    }
}
