# Bank Transaction Processor Demo / Testing

A stateful Apache Flink application that processes real-time bank transactions, 
tracks account balances, and flags overdrafts. This is meant to be self-contained (No Kafka or DB for
sources and sinks). 

## 🚀 Features
* **Stateful Balance Tracking:** Uses `ValueState` to maintain real-time account balances.
* **Overdraft Detection:** Automatically flags transactions that result in a negative balance.
* **High-Value Alerts:** Identifies transactions above a configurable threshold.
* **Checkpointing:** Fault-tolerant state that survives job restarts.

## 🛠️ Setup & Running
1. **Prerequisites:** Java 11+, Maven, and an IDE (IntelliJ recommended).
2. **Build:** Run `mvn clean install` in the terminal.
3. **Execute:** Run the `JobRUnner` class from your IDE.

## 📊 Logic Flow
The pipeline follows this structure:
`Source -> KeyBy(AccountId) -> Map(Stateful Logic) -> Sink(Print)`

## 📅 Roadmap / To-Do
1. Configurable total transaction count 
2. Clean up and add better comments
3. Remove warnings and deprecated code

