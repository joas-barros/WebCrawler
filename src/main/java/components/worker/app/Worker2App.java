package components.worker.app;

import components.worker.src.Worker;
import utils.Ports;

public class Worker2App {
    static void main(String[] args) {
        new Worker("localhost",
                Ports.ORCHESTRATOR,
                "localhost",
                Ports.DATASERVER,
                4);
    }
}
