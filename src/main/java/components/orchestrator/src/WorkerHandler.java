package components.orchestrator.src;

import utils.Color;

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
                            System.out.println(Color.infoMessage("[ORCH] Enviando tarefa para Worker: ") + nextUrl);
                        }

                        orchestrator.tryShutdown();
                    }
                } catch (Exception e) {
                    System.out.println(Color.errorMessage("[ORCH] Thread escritora do Worker finalizada."));
                }
            });

            // 2. A thread principal vira a LEITORA (Processa respostas)
            String response;
            // readLine() bloqueia até o Worker enviar uma resposta
            while (orchestrator.isRunning() && (response = in.readLine()) != null) {

                if (response.contains("FOUND:")) {
                    processWorkerResponse(response);
                    orchestrator.incrementSuccessCount();
                    activeTasks.decrementAndGet();
                } else if (response.contains("FAILED:")) {

                    String [] parts = response.split(" ");
                    String id = parts[0];
                    String failedUrl = parts[2];
                    System.out.println(Color.errorMessage("[ORCH] " + id + " falhou ao processar URL: ") + failedUrl);
                    activeTasks.decrementAndGet();

                } else {
                    System.out.println(Color.warningMessage("[ORCH] Resposta inesperada do Worker: ") + response);
                }

                orchestrator.tryShutdown();
            }

            // Se o loop de leitura quebrar (ex: worker desconectou), interrompemos a escrita
            writerThread.interrupt();
        } catch (IOException e) {
            if (orchestrator.isRunning()) {
                System.out.println(Color.errorMessage("[ORCH] Worker desconectado inesperadamente."));
            }
        }
    }

    private void processWorkerResponse(String response) {
        String[] parts = response.split("FOUND:");
        if (parts.length < 2) {
            System.out.println(Color.warningMessage("[ORCH] Resposta mal formatada do Worker: ") + response);
            return;
        }

        String workerId = response.split(" ")[0];
        String[] fromParts = parts[1].split("FROM");
        String linksPart = fromParts[0].trim();
        String urlAndLabelPart = fromParts[1].trim();

        String label = extractLabel(urlAndLabelPart);
        orchestrator.incrementLabelCount(label);

        handleFoundLinks(linksPart, workerId, label);
    }

    private String extractLabel(String urlAndLabelPart) {
        int bracketStart = urlAndLabelPart.indexOf('[');
        int bracketEnd = urlAndLabelPart.indexOf(']');

        if (bracketStart != -1 && bracketEnd != -1) {
            return urlAndLabelPart.substring(bracketStart + 1, bracketEnd);
        }

        // Retorno padrão caso a página não tenha enviado o formato com colchetes
        return "OUTROS";
    }

    private void handleFoundLinks(String linksPart, String workerId, String label) {
        String[] foundLinks = linksPart.split(",");

        System.out.println(Color.highlight("[ORCH] " + workerId + " encontrou links: ")
                + String.join(", ", foundLinks) + " | Categoria: " + label);

        for (String link : foundLinks) {
            link = link.trim();

            if (!link.isEmpty() && visitedUrls.add(link)) {
                System.out.println(Color.successMessage("[ORCH] Adicionando nova URL à fila: ") + link);
                urlQueue.add(link);
            }
        }
    }
}
