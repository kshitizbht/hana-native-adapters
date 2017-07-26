package realtimetest;

import java.util.ArrayList;
import java.util.List;

import com.sap.hana.dp.adapter.sdk.AdapterAdmin;
import com.sap.hana.dp.adapter.sdk.AdapterCDCRow;
import com.sap.hana.dp.adapter.sdk.AdapterCDCRowSet;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.RowType;
import com.sap.hana.dp.adapter.sdk.AdapterException;

public class Producer implements Runnable {

    private int frequency;
    private boolean isActive;
    private Subscription subscription;

    public Producer(Subscription subscription, int frequency) {
        this.subscription = subscription;
        this.frequency = frequency;
        this.isActive = true;
    }

    @Override
    public void run() {
        int counter = 0;
        while (isActive) {
            try {
                RealTimeAdapter.logger.warn("Waiting for " + frequency / 1000 + " seconds");
                Thread.sleep(frequency);
                List<AdapterCDCRowSet> rowSets = new ArrayList<AdapterCDCRowSet>();

                AdapterCDCRowSet beginRow = AdapterAdmin.createBeginTransactionRowSet(null, null);
                rowSets.add(beginRow);

                AdapterCDCRowSet rowSet =
                        new AdapterCDCRowSet(subscription.getHeaders(), subscription.getMetadata());
                AdapterCDCRow insertRow = rowSet.newCDCRow(RowType.INSERT);
                for (int i = 0; i < subscription.getMetadata().size(); i++) {
                    insertRow.setColumnValue(i, "" + ++counter);
                }
                rowSets.add(rowSet);

                // Send commit
                AdapterCDCRowSet commitRowSet =
                        AdapterAdmin.createCommitTransactionRowSet(null, null);
                rowSets.add(commitRowSet);

                // Add this batch of data on the queue.
                subscription.getQueue().add(rowSets);

            } catch (AdapterException e) {
                RealTimeAdapter.logger.warn("Shutting down producer thread");
                RealTimeAdapter.logger.warn(e);
                isActive = false;
            } catch (InterruptedException e) {
                RealTimeAdapter.logger.warn("Shutting down producer thread");
                RealTimeAdapter.logger.warn(e);
                isActive = false;
            }
        }
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean isActive) {
        this.isActive = isActive;
    }


}
