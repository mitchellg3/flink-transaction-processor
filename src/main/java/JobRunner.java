
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The main entry point for the Flink application.
 * This class handles environment setup and calls the core processing logic.
 */
public class JobRunner {

    private static final String JOB_NAME = "Transaction Processor with State";

    public static void main(String[] args) throws Exception {

        // 1. Set up the execution environment
        // Flink automatically decides between LocalStreamEnvironment (IDE)
        // or RemoteStreamEnvironment (Cluster) based on how it's executed.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 2. Call the dedicated pipeline builder
        TransactionProcessor.execute(env, JOB_NAME);
    }
}