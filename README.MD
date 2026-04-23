# Distributed Web Crawler Simulation

This project consists of a **Distributed Web Crawler** simulation designed for the Concurrent and Distributed Programming course. The system uses a TCP socket-based architecture and high-performance threads to explore a "mock" internet provided by a data server.

## 🚀 Features

- **Distributed Architecture**: Clear separation between the Orchestrator, Data Server, and multiple Workers.
- **Advanced Concurrency**: Use of **Java Virtual Threads** for efficient management of blocking I/O.
- **Full-Duplex Protocol**: Simultaneous bidirectional communication between the Orchestrator and Workers to avoid idle time.
- **Load Simulation**:
    - **I/O Bound**: Simulated network latency on the data server.
    - **CPU Bound**: Simulated content processing with random delays (2s to 5s) in the Workers.
- **Content Analysis**: Automatic classification of pages into labels such as Technology, Sports, Politics, etc.
- **Resilience**: Automatic retry mechanism for Workers in case of connection failure.
- **Reports**: Final table with processing statistics, total time, and category distribution.

## 🏗️ System Architecture

The system is composed of three main components:

1.  **DataServer**: Acts as the "Internet". It loads a dataset (CSV/TSV) and responds to requests for links from specific URLs.
2.  **Orchestrator**: The "brain" of the crawler. It manages the queue of pending URLs (Frontier), controls duplicates, and distributes tasks to connected Workers.
3.  **Worker**: The component that does the heavy lifting. It requests data from the DataServer, processes the simulated "HTML", validates links, and returns the results to the Orchestrator.

## 🛠️ Technologies Used

- **Language**: Java 21 (required for Virtual Threads).
- **Communication**: TCP Sockets (`ServerSocket` and `Socket`).
- **Synchronization**: `BlockingQueue`, `ConcurrentHashMap`, `AtomicInteger`, and `synchronized` blocks.
- **Dataset**: Tab/comma-delimited CSV/TSV files.

## 📋 Communication Protocol

### Worker ↔️ DataServer
- **Request**: `GET /<worker_id>/<url> HTTP/1.1`
- **Response**: `LINKS: link1, link2...` or `NOT_FOUND`

### Worker ↔️ Orchestrator
- **Command**: `PROCESS <url>`
- **Feedback**: `<id> FOUND: <links> FROM <url>[<label>]` or `<id> FAILED: <url>`

## 🚦 How to Run

### 1. Start the DataServer
Run the `DataserverApp` class. It will load the dataset and wait for connections on the configured port (e.g., 9090).

### 2. Start the Orchestrator
Run the `OrchestratorApp` class. It will initialize the seed URLs and wait for Workers to connect on the configured port (e.g., 8080).

### 3. Start the Workers
Run the Worker classes (e.g., `Worker1App`, `Worker2App`, etc.). Each Worker can be configured with a different thread capacity to test load balancing.

## 📊 Example Output (Final Report)

When the termination criteria are met (empty queue and all workers idle), the Orchestrator generates the following report:

```text
--- PAGE CONTENT ANALYSIS ---
+-----------------+------------+---------+
| LABEL           | QUANTITY   |    %    |
+-----------------+------------+---------+
| TECHNOLOGY      |        350 |  35.00% |
| SCIENCE         |        210 |  21.00% |
| BUSINESS        |        180 |  18.00% |
| OTHERS          |        100 |  10.00% |
+-----------------+------------+---------+
```