package components.worker.app;

import components.worker.src.Worker;
import utils.Ports;

public class Worker3App {
    static void main(String[] args) {
        new Worker("localhost",
                Ports.ORCHESTRATOR,
                "localhost",
                Ports.DATASERVER,
                3,
                "Worker-3");
    }
}
