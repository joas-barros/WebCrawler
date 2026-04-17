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
}