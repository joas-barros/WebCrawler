package components.worker.app;

import components.worker.src.Worker;
import utils.Ports;

public class Worker2App {
    static void main(String[] args) {
        new Worker(Ports.ORCHESTRATOR_HOST,
                Ports.ORCHESTRATOR,
                Ports.DATASERVER_HOST,
                Ports.DATASERVER,
                4,
                "Worker-2");
    }
}
