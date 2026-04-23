package components.dataserver.app;

import components.dataserver.src.Dataserver;
import utils.Ports;

public class DataserverApp {
    static void main() {
            Dataserver dataserver = new Dataserver(Ports.DATASERVER);

            dataserver.start("src/main/resources/data/webcrawler_dataset.tsv");
    }
}
