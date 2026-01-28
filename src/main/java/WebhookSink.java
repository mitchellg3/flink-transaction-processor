import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class WebhookSink extends RichSinkFunction<TransactionProcessor.Transaction> {
    private transient HttpClient client;
    private final String url = "https://webhook.site/a1b731f8-6003-41f0-948a-6dd9c8c3fa3f";

    @Override
    public void open(Configuration parameters) {
        // Initialize the client once per TaskManager subtask
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    public void invoke(TransactionProcessor.Transaction value, Context context) {
        try {
            //Currently commented out so I dont spam the webhook site
            // Convert your transaction to a simple JSON string
//            String json = value.toString(); // Or use Jackson/Gson for proper JSON
//
//            HttpRequest request = HttpRequest.newBuilder()
//                    .uri(URI.create(url))
//                    .header("Content-Type", "application/json")
//                    .POST(HttpRequest.BodyPublishers.ofString(json))
//                    .build();
//
//            // Send async to avoid blocking the Flink pipeline too much
//            client.sendAsync(request, HttpResponse.BodyHandlers.ofString());

        } catch (Exception e) {
            // Log errors but don't kill the job
            System.err.println("Failed to send to webhook: " + e.getMessage());
        }
    }
}