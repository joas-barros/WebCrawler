package components.dataserver.src;

import model.WebSite;
import utils.Color;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/*
Servidor de dados: Um processo que escuta em uma porta e, ao receber o nome de uma "URL", devolve
uma lista de strings (links) via sockets.
○ Estrutura interna: Um Map<String, WebSite> que mapeia URLs para seus dados.
○ Concorrência: Utiliza um ExecutorService para gerenciar cada conexão de Worker que chega.
Cada requisição de "GET LINKS" roda em uma thread separada para não travar o servidor.
 */
public class Dataserver {

    private final int port;

    private final Map<String, WebSite> linksMapper = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    public Dataserver(int port) {
        this.port = port;
    }

    public void loadDataset(String filePath) {

        String fileFormat = filePath.toLowerCase().endsWith(".csv") ? "CSV" :
                            filePath.toLowerCase().endsWith(".tsv") ? "TSV" : "DESCONHECIDO";

        System.out.println(Color.infoMessage("[DataServer] Carregando " + fileFormat + ": " + filePath));

        try {
            List<String> lines = Files.readAllLines(Path.of(filePath));

            int startLine = (!lines.isEmpty() && lines.get(0).toLowerCase().startsWith("id")) ? 1 : 0;

            for (int i = startLine; i < lines.size(); i++) {
                String line = lines.get(i).trim();
                if (line.isBlank()) continue;

                // id ; conteudo ; links
                String[] parts = line.split("\t", 3);
                if (parts.length < 3) {
                    System.out.println(Color.warningMessage(
                            "[DataServer] Linha " + (i + 1) + " ignorada (formato inválido): " + line));
                    continue;
                }

                String url        = parts[0].trim();
                String contentHTML = parts[1].trim();
                String linksRaw   = parts[2].trim();

                List<String> links;

                if (fileFormat .equals("TSV")) {
                    links = Arrays.stream(linksRaw.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isBlank())
                            .toList();
                } else if (fileFormat.equals("CSV")) {
                    links = Arrays.stream(linksRaw.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isBlank())
                            .toList();
                } else {
                    System.out.println(Color.warningMessage("[DataServer] Formato de arquivo desconhecido."));
                    continue;
                }

                linksMapper.put(url, new WebSite(url, contentHTML, links));
            }

            System.out.println(Color.successMessage(
                    "[DataServer] " + fileFormat + " carregado. " + linksMapper.size() + " página(s) indexada(s)."));

            System.out.println(Color.infoMessage("[DataServer] Conteúdo do linksMapper:"));
            linksMapper.forEach((url, site) -> {
                System.out.println("URL: " + url);
                System.out.println("Conteúdo HTML: " + site.getContentHTML());
                System.out.println("Links: " + site.getLinks());
                System.out.println("-----");
            });

        } catch (IOException e) {
            System.out.println(Color.errorMessage("[DataServer] Erro ao ler " + fileFormat + ": " + e.getMessage()));
            throw new UncheckedIOException(e);
        }
    }

    // Protocolo
    // Worker -> Servidor: GET /google.com HTTP/1.1
    // Servidor -> Worker: LINKS: gmail.com, youtube.com, maps.com
    private void workerHandleRequest(Socket clientSocket) {
        try (
                BufferedReader in  = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter    out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()), true)
        ) {
            String request = in.readLine();
            if (request == null || request.isBlank()) return;

            System.out.println(Color.infoMessage("[DataServer] Requisição recebida: " + request));

            String[] parsedData = parseUrl(request);

            if (parsedData == null) {
                out.println("ERROR: Requisição mal formatada. Use: GET /<id>/<url> HTTP/1.1");
                System.out.println(Color.warningMessage("[DataServer] Requisição mal formatada: " + request));
                return;
            }

            String workerId = parsedData[0];
            String url = parsedData[1];

            System.out.println(Color.infoMessage("[DataServer] " + workerId + " solicitou a URL: " + url));

            WebSite site = linksMapper.get(url);

            if (site == null) {
                out.println("NOT_FOUND: " + url);
                System.out.println(Color.warningMessage("[DataServer] URL não encontrada: " + url));
                return;
            }

            String linksJoined = String.join(", ", site.getLinks());
            out.println("LINKS: " + linksJoined);

            System.out.println(Color.successMessage(
                    "[DataServer] Respondido " + url + " → " + site.getLinks().size() + " link(s)"));

        } catch (IOException e) {
            System.out.println(Color.errorMessage("[DataServer] Erro ao tratar Worker: " + e.getMessage()));
        } finally {
            try { clientSocket.close(); } catch (IOException ignored) {}
        }
    }

    // GET /<id>/<url> HTTP/1.1
    private String[] parseUrl(String request) {
        String[] tokens = request.split(" ");
        if (tokens.length < 2 || !tokens[0].equalsIgnoreCase("GET")) return null;

        String path = tokens[1];
        if (!path.startsWith("/")) return null;

        String[] pathParts = path.substring(1).split("/", 2);
        if (pathParts.length < 2) return null;

        return new String[]{pathParts[0].trim(), pathParts[1].trim()};
    }

    public void start(String csvFilePath) {
        loadDataset(csvFilePath);

        System.out.println(Color.title(
                "[DataServer] Iniciando na porta " + port + "..."));

        try (ServerSocket serverSocket = new ServerSocket(port)) {

            System.out.println(Color.successMessage(
                    "[DataServer] Pronto. Aguardando conexões na porta " + port + "..."));

            // Aceita conexões indefinidamente
            while (!serverSocket.isClosed()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println(Color.infoMessage(
                            "[DataServer] Nova conexão: " + clientSocket.getInetAddress()));

                    // Cada Worker recebe sua própria virtual thread
                    executorService.submit(() -> workerHandleRequest(clientSocket));

                } catch (SocketException e) {
                    System.out.println(Color.warningMessage("[DataServer] Servidor encerrado."));
                    break;
                } catch (IOException e) {
                    System.out.println(Color.errorMessage("[DataServer] Erro ao aceitar conexão: " + e.getMessage()));
                }
            }

        } catch (IOException e) {
            System.out.println(Color.errorMessage("[DataServer] Falha ao abrir porta " + port + ": " + e.getMessage()));
            throw new UncheckedIOException(e);
        } finally {
            executorService.shutdown();
            System.out.println(Color.infoMessage("[DataServer] ExecutorService encerrado."));
        }
    }
}