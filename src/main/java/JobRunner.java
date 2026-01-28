
import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The main entry point for the Flink application.
 * This class handles environment setup and calls the core processing logic.
 */
public class JobRunner {

    private static final String JOB_NAME = "Transaction Processor with State";

    public static void main(String[] args) throws Exception {

        // Parse parameters from command line
        ParameterTool params = ParameterTool.fromArgs(args);

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read 'checkpoint.interval' from params, default to 60s if missing
        long checkpointInterval = params.getLong("checkpoint.interval", 60000);
        env.enableCheckpointing(checkpointInterval);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Get the limit (default to 4000 if not provided)
        int maxAccounts = params.getInt("max.accounts", 4000);

        boolean bloatState = params.getBoolean("state.bloat", false); // Default to false if not passed

        // Make parameters available globally in your operators (optional)
        env.getConfig().setGlobalJobParameters(params);
        Log.info("Using Parameters: max.accounts = " + maxAccounts + " & checkpoint.interval = " +  checkpointInterval + " & state.bloat = " + bloatState);

        // Call the dedicated pipeline builder
        TransactionProcessor.execute(env, JOB_NAME, maxAccounts, bloatState);
    }
}