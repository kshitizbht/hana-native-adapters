package realtimetest;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.sap.hana.dp.adapter.sdk.AdapterCDCRowSet;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.ReceiverConnection;

public class Consumer implements Runnable {


    private boolean isActive;
    private ReceiverConnection receiverConnection;
    private Subscription subscription;

    public Consumer(Subscription subscription, ReceiverConnection receiverConnection) {
        this.isActive = true;
        this.receiverConnection = receiverConnection;
        this.subscription = subscription;
    }

    @Override
    public void run() {
        while (isActive) {
            List<AdapterCDCRowSet> rowSets;
            try {
                rowSets = this.subscription.getQueue().poll(5000, TimeUnit.SECONDS);
                if (rowSets != null) {
                    RealTimeAdapter.logger.info("Consumer sending rows " + rowSets.size());
                    // add sequence id and transaction id so that HANA applies it correctly.
                    // Basically one rowset is a single transaction and each row in that row will
                    // now need to have increment sequence number.
                    byte[] tranId = RealTimeAdapter.getNextTransactionId();
                    for (AdapterCDCRowSet rowSet : rowSets) {
                        for (int i = 0; i < rowSet.getRowCount(); i++) {
                            rowSet.getCDCRow(i).setSeqID(RealTimeAdapter.getNextSequenceId());
                            rowSet.getCDCRow(i).setTransactionId(tranId);
                        }
                        receiverConnection.sendRowSet(rowSet); // Send the rows to HANA.
                    }
                }
            } catch (InterruptedException e) {
                RealTimeAdapter.logger.warn("Shutting down producer thread");
                RealTimeAdapter.logger.warn(e);
                isActive = false;
            } catch (AdapterException e) {
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
