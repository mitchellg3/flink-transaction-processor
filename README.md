# Bank Transaction Processor Demo / Testing

A stateful Apache Flink application that processes real-time bank transactions, 
tracks account balances, and flags overdrafts. This is meant to be self-contained (No Kafka or DB for
sources and sinks). **This is built on Flink 1.20**

## 🚀 Features
* **Stateful Balance Tracking:** Uses Flink's `ValueState` Utilizes Flink's ValueState to maintain a distributed "source of truth" for account balances, ensuring sub-millisecond lookups.
* **Overdraft Detection:** Automatically flags transactions that result in a negative balance.
* **High-Value Alerts:** Identifies transactions above a configurable threshold.
* **Checkpointing:** Fault-tolerant state that survives job restarts.
* **Exactly-Once Guarantees:** Configured with Checkpointing to ensure that even if the cluster crashes, account balances remain accurate and no data is double-counted. Currently every 1 min, but if this was real you should checkpoint more often.
* **Self-Contained Testing:** Includes a built-in SourceFunction that generates mock banking traffic, making it 100% runnable out of the box.
* **Real-time Filtering**: The pipeline automatically bifurcates the data stream using a side-logic filter. It classifies transactions into `Standard`, `HighAmountTransaction`, or `OVERDRAFT_WARNING`.

## 📦 Quick Start: Run the JAR

If you want to skip the build process and run the application directly on Flink or Ververica Cloud, you can download the pre-compiled shaded JAR from the Releases page.

1. **Download the latest JAR:** Go to [Releases](https://github.com/mitchellg3/flink-transaction-processor/releases) and download the file named `flink-transaction-processor-latest.jar`.

## 🛠️ Setup & Running
1. **Prerequisites:** Java 11+, Maven, and an IDE (IntelliJ recommended).

2. **Clone the repo:** `git clone https://github.com/mitchellg3/flink-transaction-processor.git
   cd flink-transaction-processor`

3. **Build:** Run `mvn clean install` in the terminal.

4. **Execute:** Run the `JobRunner` class from your IDE.

## 🏗️ Architecture & Logic Flow
The pipeline follows this structure:
`Source -> KeyBy(AccountId) -> Map(Stateful Logic) -> Sink(Print)`

**Ingestion:** Mock transactions (Account ID, Amount, Timestamp) are generated.

**Partitioning (keyBy):** Data is routed by AccountId. All transactions for a specific user are guaranteed to be processed by the same parallel worker.

**Stateful Processing:** A RichMapFunction retrieves the current balance from Flink’s managed memory, applies the transaction, checks for overdrafts, and updates the state.

**Egress:** Results are streamed to the console (standard out) for real-time monitoring.

## ⚙️ Configuration Parameters

The application uses Flink's `ParameterTool` to allow dynamic configuration. You can pass these arguments via the command line or the "Main Arguments" section in Ververica Cloud.

### 📊 Runtime Arguments

| Parameter | Type      | Default | Optional | Description                                                                                                              |
| :--- |:----------|:--------|:---------|:-------------------------------------------------------------------------------------------------------------------------|
| `--max.accounts` | `int`     | `4000`  | ✅    | **Key Cardinality:** Controls the number of unique account IDs. Higher values increase the "width" of the Managed State. |
| `--checkpoint.interval` | `long`    | `60000` | ✅   | **Fault Tolerance:** Interval (in ms) between state snapshots. (e.g., `30000` for 30s).                                  |
| `--state.bloat` | `boolean` | `false` | ✅    | **Increase State Size:** Every Transaction object stored in Flink's ValueState is padded with a 2KB string of junk data                                                                                                |

---
## 📅 Roadmap / To-Do
1. Remove warnings and deprecated code
2. Clean up and add better comments
