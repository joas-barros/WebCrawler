package components.worker.src;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class MockWorker {
    public static void main(String[] args) {
        System.out.println("Iniciando Mock Worker...");

        try (Socket socket = new Socket("localhost", 8080);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            System.out.println("Conectado ao Orquestrador!");

            String command;
            // Fica ouvindo os comandos do Orquestrador
            while ((command = in.readLine()) != null) {
                System.out.println("Recebi: " + command);

                if (command.startsWith("PROCESS ")) {
                    String urlToProcess = command.substring(8);

                    // Simula o tempo de I/O (baixar a página)
                    Thread.sleep(1000);

                    // Lógica fake: Se for google, acha 2 links. Se for outro, não acha nada (para a fila poder esvaziar e o teste acabar)
                    if (urlToProcess.equals("google.com")) {
                        System.out.println("-> Simulando extração e enviando links...");
                        out.println("FOUND: ufersa.edu.br, linux.org FROM google.com");
                    } else {
                        System.out.println("-> Simulando página sem links (beco sem saída)...");
                        out.println("FOUND:  FROM " + urlToProcess);
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Conexão encerrada pelo servidor.");
        }
    }
}
