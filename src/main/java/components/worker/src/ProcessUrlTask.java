package components.worker.src;

import model.WebSite;
import utils.Color;
import utils.Labels;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ProcessUrlTask implements Runnable {

    private final String url;
    private final String dataServerHost;
    private final int dataServerPort;
    private final PrintWriter orchestratorOut;
    private final String identification;
    private final AtomicInteger processedCounter;
    private final Map<String, Labels> urlReport;

    public ProcessUrlTask(String url, String dataServerHost, int dataServerPort, PrintWriter orchestratorOut, String identification, AtomicInteger processedCounter, Map<String, Labels> urlReport) {
        this.url = url;
        this.dataServerHost = dataServerHost;
        this.dataServerPort = dataServerPort;
        this.orchestratorOut = orchestratorOut;
        this.identification = identification;
        this.processedCounter = processedCounter;
        this.urlReport = urlReport;
    }

    @Override
    public void run() {
        System.out.println(Color.infoMessage("[WorkerTask] Iniciando processamento da URL: " + url));

        try (Socket dataServerSocket = new Socket(dataServerHost, dataServerPort);
             PrintWriter out = new PrintWriter(new OutputStreamWriter(dataServerSocket.getOutputStream()), true);
             ObjectInputStream in = new ObjectInputStream(dataServerSocket.getInputStream())) {

            String outputMessage = "GET /" + identification + "/" + url + " HTTP/1.1";
            System.out.println(Color.infoMessage("[WorkerTask] Enviando requisição para DataServer: " + outputMessage));

            out.println(outputMessage);

            Object response = in.readObject();

            if (response instanceof WebSite) {
                WebSite site = (WebSite) response;

                String contentHtml = site.getContentHTML().toLowerCase();

                Map<Labels, Predicate<String>> categoryRules = Map.of(
                        Labels.TECNOLOGIA,    html -> html.contains("algoritmo") || html.contains("software") || html.contains("programacao"),
                        Labels.ESPORTES,      html -> html.contains("futebol") || html.contains("campeonato") || html.contains("gol"),
                        Labels.POLITICA,      html -> html.contains("governo") || html.contains("eleicao") || html.contains("presidente"),
                        Labels.SAUDE,         html -> html.contains("medicina") || html.contains("vacina") || html.contains("hospital"),
                        Labels.ENTRETENIMENTO,html -> html.contains("cinema") || html.contains("streaming") || html.contains("musica"),
                        Labels.CIENCIA,       html -> html.contains("pesquisa") || html.contains("universo") || html.contains("genetica"),
                        Labels.NEGOCIOS,      html -> html.contains("mercado") || html.contains("investimento") || html.contains("empresa")
                );

                Labels label = categoryRules.entrySet().stream()
                        .filter(entry -> entry.getValue().test(contentHtml))
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElse(Labels.OUTROS);

                site.setLabel(label);

                urlReport.put(url, label);

                simulateCpuBoundWork();

                List<String> validLinks = validateLinks(site.getLinks());
                String linksJoined = String.join(", ", validLinks).replaceAll("^\\[|\\]$", "");

                String resultMessage = identification + " FOUND: " + linksJoined + " FROM " + url + "[" + label + "]";

                synchronized (orchestratorOut) {
                    System.out.println(Color.infoMessage("[WorkerTask] Enviando resultado para Orquestrador: " + resultMessage));
                    orchestratorOut.println(resultMessage);
                }

                System.out.println(Color.successMessage("[WorkerTask] Sucesso: " + url + " -> " + validLinks.size() + " link(s) validados."));

            } else if ("NOT_FOUND".equals(response)) {
                System.out.println(Color.errorMessage("[WorkerTask] Erro 404, URL não existe no banco: " + url));
                urlReport.put(url, Labels.ERROR);
                synchronized (orchestratorOut) {
                    orchestratorOut.println(identification + " FAILED: " + url);
                }
            } else {
                System.out.println(Color.errorMessage("[WorkerTask] Resposta inesperada do DataServer para URL " + url + ": " + response));
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println(Color.errorMessage("[WorkerTask] Erro ao conectar com DataServer para URL " + url + ": " + e.getMessage()));
        } finally {
            processedCounter.incrementAndGet();
        }
    }

    private void simulateCpuBoundWork() {
        try {
            long sleepTime = ThreadLocalRandom.current().nextLong(2000, 5001);
            System.out.println(Color.infoMessage("[WorkerTask] Processando conteúdo da página (CPU Bound)..."));
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println(Color.errorMessage("[WorkerTask] Processamento CPU Bound interrompido para " + url));
        }
    }

    private List<String> validateLinks(List<String> rawLinks) {
        if (rawLinks == null || rawLinks.isEmpty()) {
            return List.of();
        }

        return rawLinks.stream()
                .filter(link -> link != null && !link.isBlank())
                .filter(link -> !link.equalsIgnoreCase(this.url))
                .collect(Collectors.toList());
    }
}