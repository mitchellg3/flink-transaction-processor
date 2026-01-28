
import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.Serializable;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

/**
 * Contains the Data Model and the Flink pipeline building logic.
 * The core logic is isolated here.
 */

public class TransactionProcessor {

    // --- Data Model (Serializable for Flink) --
    public static class Transaction implements Serializable {
        private static final Logger LOG = LoggerFactory.getLogger(TransactionProcessor.class);

        public long transactionId;
        public String accountId;
        public double amount;
        public long timestamp;
        public String processingStatus;
        public double accountBalance;
        public double newBalance;

        public Transaction() {}

        public Transaction(long transactionId, String accountId, double amount, long timestamp, double accountBalance, double newBalance) {
            this.transactionId = transactionId;
            this.accountId = accountId;
            this.amount = amount;
            this.timestamp = timestamp;
            this.accountBalance = accountBalance;
            this.newBalance = newBalance;
        }

        @Override
        public String toString() {
            return String.format(
                    "Transaction{id=%d, account='%s', amount=%.2f, time=%d, Current_Balance='%.2f', New_Balance='%.2f', status='%s'}",
                    transactionId, accountId, amount, timestamp, accountBalance, newBalance, processingStatus
            );
        }
    }

    // --- Continuous Transaction Source ---

    public static class ContinuousTransactionSource implements ParallelSourceFunction<Transaction> {


        //This is the system exit. If you click "Cancel" or "Stop" in the UI, or if the Flink cluster needs to shut down for maintenance,
        // Flink calls the cancel() method. If you don't have isRunning in your loop, the source might keep trying to generate data for a few seconds during the shutdown process,
        // which can lead to messy logs or interrupted checkpoints.
        private volatile boolean isRunning = true;

        private long startTransactionIdAt = 200045; // Just looks more real
        private final Random random = new Random();
        private final int maxAccounts;


        public ContinuousTransactionSource(int maxAccounts) {
            this.maxAccounts = maxAccounts;
        }

        @Override
        public void run(SourceFunction.SourceContext<Transaction> ctx) throws Exception {
            while (isRunning) {

                // Generate new transaction data
                long txId = startTransactionIdAt++;

                // ensures that every randomAccountNum generated will be a unique-looking 5-digit ID starting with the number 1 (from 10,000 to 13,999)
                // basically 4000 different account possibilities
                int randomAccountNum = 10000 + random.nextInt(maxAccounts);
                String accountId = "ACCT-" + randomAccountNum;

                // Generate a random amount for a transaction between 1.00 and 1000.00 for base amount
                double baseTransactionAmount = 1.0 + (1000.0 - 1.0) * random.nextDouble();

                // Introduce a chance for the amount to be negative (e.g., 20% chance)
                double amount;
                if (random.nextDouble() < 0.20) {
                    // 20% chance: make it negative (withdrawal/refund)
                    amount = -baseTransactionAmount;
                } else {
                    // 80% chance: keep it positive (deposit/credit)
                    amount = baseTransactionAmount;
                }

                long timestamp = System.currentTimeMillis();

                Transaction newTransaction = new Transaction(txId, accountId, amount, timestamp, 0.00, 0.00);

                // Emit the transaction with a synchronized lock
                // This lock is important for correct checkpointing
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(newTransaction);
                }

                /*
                 Wait for 1 second before generating the next transaction
                TimeUnit.SECONDS.sleep(1);
                TimeUnit.MILLISECONDS.sleep(1); // 1000 trx per sec
                */
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void execute(StreamExecutionEnvironment env, String jobName, int maxAccounts) throws Exception {

        // Read data from the new Continuous Source
        // This is now an UNBOUNDED source, meaning the job will never finish.
        DataStream<Transaction> transactionStream = env.addSource(new ContinuousTransactionSource(maxAccounts))
                .name("Continuous Transaction Source");

        // Add the new columns (processingStatus, currentBalance, and NewBalance) using a RichMapFunction
        // Use RichMapFunction over Mapfunction since this uses state. MapFunction can not see the Runtime Context
        DataStream<Transaction> processedStream = transactionStream
                .keyBy(transaction -> transaction.accountId) // Group by account
                .map(new RichMapFunction<Transaction, Transaction>() {

                    // The state handle: Flink manages this for us
                    private transient ValueState<Double> runningBalance;

                    @Override
                    public void open(Configuration parameters) {
                        // Initialize the state descriptor
                        ValueStateDescriptor<Double> descriptor =
                                new ValueStateDescriptor<>("accountBalance", Double.class, 0.0);
                        runningBalance = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Transaction map(Transaction transaction) throws Exception {
                        // Get the current balance from state
                        Double currentBalance = runningBalance.value();
                        if (currentBalance == null) { currentBalance = 0.0; }

                        // Update the balance with the new transaction amount
                        double newBalance = currentBalance + transaction.amount;
                        runningBalance.update(newBalance);

                        // Add logic using that state (Example: Status Flag if account goes negative or an expensive purchase)
                        if (newBalance < 0) {
                            transaction.processingStatus = "OVERDRAFT_WARNING";
                        } else if (transaction.amount > 700.00 && newBalance > 0) {
                            transaction.processingStatus = "HighAmountTransaction";
                        } else {
                            transaction.processingStatus = "Standard";
                        }

                        transaction.newBalance = newBalance;
                        transaction.accountBalance = currentBalance;

                        // Optional: Print balance to console for debugging
                        // System.out.println("Account: " + transaction.accountId + " | New Balance: " + newBalance);

                        return transaction;
                    }
                })
                .returns(Transaction.class)
                .name("Stateful Balance Tracker");

        // Output to the log
        processedStream.print("Bank Transaction");

        // Execute the Flink job
        Log.info("Starting Flink Job Execution...");
        env.execute(jobName);
        Log.info("Flink Job Started and Running Continuously...");
    }

}


