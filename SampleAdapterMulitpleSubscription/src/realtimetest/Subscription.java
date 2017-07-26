package realtimetest;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.sap.hana.dp.adapter.sdk.AdapterCDCRowSet;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.ReceiverConnection;
import com.sap.hana.dp.adapter.sdk.SubscriptionSpecification;
import com.sap.hana.dp.adapter.sdk.TableMetadata;

public class Subscription {

    String headers;
    List<Column> metadata;
    TableMetadata table;
    SubscriptionSpecification spec;
    BlockingQueue<List<AdapterCDCRowSet>> queue;
    private Producer producer;
    private Thread producerThread;
    private Consumer consumer;
    private Thread consumerThread;

    public Subscription(String header, TableMetadata table, SubscriptionSpecification spec,
            BlockingQueue<List<AdapterCDCRowSet>> queue) {
        this.queue = queue;
        this.headers = header;
        this.table = table;
        this.spec = spec;
        this.metadata = table.getColumns();
    }

    public BlockingQueue<List<AdapterCDCRowSet>> getQueue() {
        return queue;
    }

    public void setQueue(BlockingQueue<List<AdapterCDCRowSet>> queue) {
        this.queue = queue;
    }

    public String getHeaders() {
        return headers;
    }

    public void setHeaders(String headers) {
        this.headers = headers;
    }

    public List<Column> getMetadata() {
        return metadata;
    }

    public void setMetadata(List<Column> metadata) {
        this.metadata = metadata;
    }

    public TableMetadata getTable() {
        return table;
    }

    public void setTable(TableMetadata table) {
        this.table = table;
    }

    public SubscriptionSpecification getSpec() {
        return spec;
    }

    public void setSpec(SubscriptionSpecification spec) {
        this.spec = spec;
    }

    public void startProducer() {
        producer = new Producer(this, 5000);// frequency in
        producerThread = new Thread(producer, "Random Data Generator for " + getTable().getName());
        producerThread.start();
    }

    public void startConsumer(ReceiverConnection receiver) {
        consumer = new Consumer(this, receiver);
        consumerThread = new Thread(consumer, "Data sender for " + getTable().getName());
        consumerThread.start();
    }

    public void stop() {
        if (this.producer.isActive()) {
            this.producer.setActive(false);
            this.producerThread.interrupt();
        }
        if (this.consumer.isActive()) {
            this.consumer.setActive(false);
            this.consumerThread.interrupt();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(table.getName() + " [");
        if (headers != null) {
            builder.append("headers=");
            builder.append(headers);
            builder.append(", ");
        }
        if (spec != null) {
            builder.append("spec=");
            builder.append(spec);
        }
        builder.append("]");
        return builder.toString();
    }



}
