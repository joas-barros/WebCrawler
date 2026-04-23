package components.orchestrator.app;

import components.orchestrator.src.Orchestrator;
import utils.Ports;

public class OrchestratorApp {
    static void main(String[] args) {
        new Orchestrator(Ports.ORCHESTRATOR);
    }
}
