package components.worker.app;

import components.worker.src.Worker;
import utils.Ports;

public class Worker4App {
    static void main(String[] args) {
        new Worker("localhost",
                Ports.ORCHESTRATOR,
                "localhost",
                Ports.DATASERVER,
                2,
                "Worker-4");
    }
}
