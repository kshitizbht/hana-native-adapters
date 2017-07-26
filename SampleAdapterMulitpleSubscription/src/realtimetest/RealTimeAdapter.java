package realtimetest;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sap.hana.dp.adapter.sdk.AdapterAdmin;
import com.sap.hana.dp.adapter.sdk.AdapterCDC;
import com.sap.hana.dp.adapter.sdk.AdapterCDCRowSet;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterRow;
import com.sap.hana.dp.adapter.sdk.AdapterRowSet;
import com.sap.hana.dp.adapter.sdk.AdapterStatistics;
import com.sap.hana.dp.adapter.sdk.BrowseNode;
import com.sap.hana.dp.adapter.sdk.CallableProcedure;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.DataDictionary;
import com.sap.hana.dp.adapter.sdk.FunctionMetadata;
import com.sap.hana.dp.adapter.sdk.LatencyTicketSpecification;
import com.sap.hana.dp.adapter.sdk.Metadata;
import com.sap.hana.dp.adapter.sdk.Parameter;
import com.sap.hana.dp.adapter.sdk.ParametersResponse;
import com.sap.hana.dp.adapter.sdk.ProcedureMetadata;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.ReceiverConnection;
import com.sap.hana.dp.adapter.sdk.RemoteObjectsFilter;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;
import com.sap.hana.dp.adapter.sdk.SequenceId;
import com.sap.hana.dp.adapter.sdk.StatementInfo;
import com.sap.hana.dp.adapter.sdk.SubscriptionSpecification;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.Timestamp;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;


/**
 * RealTimeTest Adapter.
 */
public class RealTimeAdapter extends AdapterCDC {

    public static Logger logger = LogManager.getLogger("RealTimeTest");
    private String name = null;
    private boolean alreadySent = false;
    private String currentNode = null;
    private int fetchSize = 100;
    private boolean autocommit;
    private boolean isCDC = false;
    private ReceiverConnection receiver;
    private ConcurrentHashMap<String, Subscription> realTimeObjectMap;
    private ConcurrentHashMap<String, String> statistics;

    @Override
    public void setBrowseNodeId(String nodeId) throws AdapterException {
        this.currentNode = nodeId;
    }


    @Override
    public List<BrowseNode> browseMetadata() throws AdapterException {
        List<BrowseNode> nodes = new ArrayList<BrowseNode>();
        if (this.currentNode == null || this.currentNode.isEmpty()) {
            BrowseNode node = new BrowseNode("SDA", "SDA");
            node.setExpandable(true);
            nodes.add(node);
            BrowseNode node2 = new BrowseNode("CDC", "CDC");
            node2.setExpandable(true);
            nodes.add(node2);
        } else if (this.currentNode.compareTo("SDA") == 0) {
            BrowseNode node = new BrowseNode("DemoTable", "DemoTable");
            node.setImportable(true);
            node.setExpandable(false);
            nodes.add(node);
        } else if (this.currentNode.compareTo("CDC") == 0) {
            BrowseNode node = new BrowseNode("RealTimeDemoTable", "RealTimeDemoTable");
            node.setImportable(true);
            node.setExpandable(false);
            nodes.add(node);
        }
        return nodes;
    }

    /*
     * End of adapter instance life cycle ProperCleanup is required.
     */
    @Override
    public void close() throws AdapterException {
        for (String key : this.realTimeObjectMap.keySet()) {
            this.realTimeObjectMap.get(key).stop();
        }
        this.realTimeObjectMap.clear();
    }

    /*
     * Interim objects like jdbc result set, file stream or other inputstream can be closed here.
     */
    @Override
    public void closeResultSet() throws AdapterException {}

    @Override
    public void executeStatement(String sql, StatementInfo info) throws AdapterException {
        alreadySent = false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.sap.hana.dp.adapter.sdk.Adapter#getCapabilities(java.lang.String)
     */
    @Override
    public Capabilities<AdapterCapability> getCapabilities(String version) throws AdapterException {
        Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
        capbility.setCapability(AdapterCapability.CAP_SELECT);
        capbility.setCapability(AdapterCapability.CAP_BIGINT_BIND); // So that we can get BIGINT
                                                                    // pushdown
        capbility.setCapability(AdapterCapability.CAP_METADATA_ATTRIBUTE); // Table/Column
                                                                           // attributes.
        capbility.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC); // To support CDC.
        return capbility;
    }


    /*
     * Since we said CAP_TRANSACTIONAL_CDC on getCapabilities, we can now just say CAP_SELECT on
     * getCDC Caps. This way, we only get full SQL.
     * 
     * (non-Javadoc)
     * 
     * @see com.sap.hana.dp.adapter.sdk.AdapterCDC#getCDCCapabilities(java.lang.String)
     */
    @Override
    public Capabilities<AdapterCapability> getCDCCapabilities(String arg0) throws AdapterException {
        Capabilities<AdapterCapability> capbility = new Capabilities<AdapterCapability>();
        capbility.setCapability(AdapterCapability.CAP_SELECT);
        capbility.setCapability(AdapterCapability.CAP_TRANSACTIONAL_CDC);
        return capbility;

    }

    @Override
    public int getLob(long lobId, byte[] bytes, int bufferSize) throws AdapterException {
        return 0;
    }

    @Override
    public void getNext(AdapterRowSet rows) throws AdapterException {
        /**
         * Currently this function is called until it returns a empty row set.
         */
        if (alreadySent)
            return;
        /**
         * Lets return a single row with 1, hello user data.
         */
        AdapterRow row = rows.newRow();
        row.setColumnValue(0, 1);
        row.setColumnValue(1, "Hello " + name);
        alreadySent = true;
    }

    @Override
    public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
        RemoteSourceDescription rs = new RemoteSourceDescription();

        PropertyGroup connectionInfo =
                new PropertyGroup("testParam", "Test Parameters", "Test Parameters");
        connectionInfo.addProperty(new PropertyEntry("name", "name"));

        CredentialProperties credentialProperties = new CredentialProperties();
        CredentialEntry credential = new CredentialEntry("credential", "Test Credentials");
        credential.getUser().setDisplayName("Demo Username");
        credential.getPassword().setDisplayName("Demo Password");
        credentialProperties.addCredentialEntry(credential);

        rs.setCredentialProperties(credentialProperties);
        rs.setConnectionProperties(connectionInfo);
        return rs;
    }


    @Override
    public Metadata importMetadata(String nodeId) throws AdapterException {
        List<Column> schema = new ArrayList<Column>();
        Column col1 = new Column("intColumn", DataType.INTEGER);
        col1.setNullable(true);
        schema.add(col1);
        Column col2 = new Column("textColumn", DataType.VARCHAR, 256);
        col2.setNullable(true);
        schema.add(col2);
        TableMetadata table = new TableMetadata();
        table.setName(nodeId);
        table.setColumns(schema);
        return table;
    }

    @Override
    public void open(RemoteSourceDescription connectionInfo, boolean isCDC)
            throws AdapterException {
        this.isCDC = isCDC;
        logger.info("Request for open with " + isCDC);
        realTimeObjectMap = new ConcurrentHashMap<String, Subscription>();
        statistics = new ConcurrentHashMap<String, String>();
        statistics.put("Adapter Started at", new Timestamp(new Date()).toString());
        String username = "";
        String password = "";
        try {
            username =
                    new String(
                            connectionInfo.getCredentialProperties()
                                    .getCredentialEntry("credential").getUser().getValue(),
                            "UTF-8");
            password =
                    new String(
                            connectionInfo.getCredentialProperties()
                                    .getCredentialEntry("credential").getPassword().getValue(),
                            "UTF-8");
        } catch (UnsupportedEncodingException e1) {
            throw new AdapterException(e1, e1.getMessage());
        }

        name = connectionInfo.getConnectionProperties().getPropertyEntry("name").getValue();
    }

    @Override
    public AdapterStatistics getAdapterStatistics() {
        // Add more information here like timestamp of row that was send
        // How many rows was send, avg time in queue, etc. Anything that could be helpful to
        // troubleshoot later.
        AdapterStatistics adapterStatistics = new AdapterStatistics(new Timestamp(new Date()));
        adapterStatistics.addStatistic("Subscriptions", "Active Subscriptions",
                this.realTimeObjectMap.size() + "");
        for (String key : this.statistics.keySet()) {
            adapterStatistics.addStatistic("Adapter", key, this.statistics.get(key));
        }
        return adapterStatistics;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public void setAutoCommit(boolean autocommit) throws AdapterException {
        this.autocommit = autocommit;
    }

    /*
     * Begin CDC Apis
     * 
     * Goal of this project is to show how we can use Real Time Replication adapter to push changes
     * from source to HANA. The API for CDC Calls will follow the following format.
     * 
     * Given a table DemoTable we want to send new insert record every N seconds.
     * 
     * On ALTER remote subscription "rt_trig1" QUEUE; If for the first time, Server calls
     * addSubscription. Since we can't maintain checkpoint we can return null here. If you can
     * maintain some sort of checkpoint then you can use this to allow recovery. Server calls start
     * call with a push handle. We can use this Receiver class to push data. Server calls
     * beginMarker() : This is to keep track of initial load and ensure data consistency when
     * initial load is in progress. Say while initial load is going on, some one deletes half of the
     * data, we should send the deletes via receiver. This way server can auto correct the data.
     * Server calls endMarker() : This is to indicate end of initial load. You should return the
     * same marker send by the server. Now you can have a thread which keeps sending changes to
     * server.
     */
    @Override
    public String addSubscription(SubscriptionSpecification arg0) throws AdapterException {
        // Since we only have one table to replicate we should however throw error on duplicate
        // subscription.

        return null; // We are not going to support recovery.
    }

    @Override
    public void removeSubscription(SubscriptionSpecification arg0) throws AdapterException {
        // Since we didn't do anything for addSubscription, we don't need to unsubscribe from
        // source.
    }

    @Override
    public void start(ReceiverConnection arg0, SubscriptionSpecification arg1)
            throws AdapterException {
        // Save push handle.
        if (this.receiver == null)
            this.receiver = arg0;

        // Create a real time object with all the necessary items like spec, metaedata etc.
        String tableName = getTableNameFromSQLStatement(arg1.getSQLStatement());
        BlockingQueue<List<AdapterCDCRowSet>> realTimeData =
                new LinkedBlockingQueue<List<AdapterCDCRowSet>>();
        Subscription subscription = new Subscription(arg1.getHeader(),
                (TableMetadata) importMetadata(tableName), arg1, realTimeData);
        this.realTimeObjectMap.put(tableName, subscription);

        logger.info("Request to start replication for " + subscription.toString());

        // Start two threads for each table
        // One thread to generate random data. You can now replace this thread logic to get proper
        // data from the source system.
        // Second thread to push this data to HANA.
        subscription.startProducer();
        subscription.startConsumer(receiver);
    }


    @Override
    public void stop(SubscriptionSpecification arg0) throws AdapterException {
        // Stop the thread that is pushing data.
        String tableName = getTableNameFromSQLStatement(arg0.getSQLStatement());
        this.realTimeObjectMap.get(tableName).stop();
    }


    @Override
    public void beginMarker(String marker, SubscriptionSpecification arg1) throws AdapterException {
        AdapterCDCRowSet rows = AdapterAdmin.createBeginMarkerRowSet(marker, new SequenceId(0));
        try {
            String tableName = getTableNameFromSQLStatement(arg1.getSQLStatement());
            List<AdapterCDCRowSet> rowsets = new ArrayList<AdapterCDCRowSet>();
            rowsets.add(rows);
            this.realTimeObjectMap.get(tableName).getQueue().put(rowsets);
        } catch (InterruptedException e) {
            throw new AdapterException(e);
        }
    }

    @Override
    public void endMarker(String marker, SubscriptionSpecification arg1) throws AdapterException {
        AdapterCDCRowSet rows = AdapterAdmin.createEndMarkerRowSet(marker, new SequenceId(0));
        try {
            String tableName = this.realTimeObjectMap.keys().nextElement();// getTableNameFromSQLStatement(arg1.getSQLStatement());
            List<AdapterCDCRowSet> rowsets = new ArrayList<AdapterCDCRowSet>();
            rowsets.add(rows);
            this.realTimeObjectMap.get(tableName).getQueue().put(rowsets);
        } catch (InterruptedException e) {
            throw new AdapterException(e);
        }
    }

    @Override
    public void executePreparedInsert(String arg0, StatementInfo arg1) throws AdapterException {
        // TODO Auto-generated method stub

    }

    @Override
    public void executePreparedUpdate(String arg0, StatementInfo arg1) throws AdapterException {
        // TODO Auto-generated method stub

    }

    @Override
    public int executeUpdate(String sql, StatementInfo info) throws AdapterException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Metadata importMetadata(String nodeId, List<Parameter> dataprovisioningParameters)
            throws AdapterException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ParametersResponse queryParameters(String nodeId, List<Parameter> parametersValues)
            throws AdapterException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<BrowseNode> loadTableDictionary(String lastUniqueName) throws AdapterException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataDictionary loadColumnsDictionary() throws AdapterException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void executeCall(FunctionMetadata metadata) throws AdapterException {
        // TODO Auto-generated method stub
    }

    @Override
    public void validateCall(FunctionMetadata metadata) throws AdapterException {
        // TODO Auto-generated method stub
    }

    @Override
    public void setNodesListFilter(RemoteObjectsFilter remoteObjectsFilter)
            throws AdapterException {
        // TODO Auto-generated method stub
    }



    @Override
    public Metadata getMetadataDetail(String arg0) throws AdapterException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CallableProcedure prepareCall(ProcedureMetadata arg0) throws AdapterException {
        // TODO Auto-generated method stub
        return null;
    }



    @Override
    public void committedChange(SubscriptionSpecification arg0) throws AdapterException {
        // TODO Auto-generated method stub

    }



    @Override
    public boolean requireDurableMessaging() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setAdapterStatisticsUpdateInterval(int arg0) {
        // TODO Auto-generated method stub

    }



    @Override
    public void startLatencyTicket(LatencyTicketSpecification arg0) throws AdapterException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean supportsRecovery() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void beginTransaction() throws AdapterException {
        // TODO Auto-generated method stub

    }

    @Override
    public void commitTransaction() throws AdapterException {
        // TODO Auto-generated method stub

    }

    @Override
    public String getSourceVersion(RemoteSourceDescription remoteSourceDescription)
            throws AdapterException {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public int putNext(AdapterRowSet rows) throws AdapterException {
        return 0;
    }

    @Override
    public void rollbackTransaction() throws AdapterException {
        // TODO Auto-generated method stub

    }


    private String getTableNameFromSQLStatement(String sql) throws AdapterException {
        if (sql == null || sql.isEmpty())
            throw new AdapterException("Empty SQL Statement received");
        List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
        Query query = (Query) ExpressionParserUtil.buildQuery(sql, messageList);
        if (query == null)
            throw new AdapterException("SQL Parse failed");
        TableReference tableRef = (TableReference) query.getFromClause();
        if (tableRef == null)
            throw new AdapterException("SQL Parse failed not a SELECT statement");
        return tableRef.getUnquotedFullName();
    }

    public static long sequenceId;
    public static long transactionId;
    public static ByteBuffer tranIdBuffer = ByteBuffer.allocate(8);

    static {
        sequenceId = System.currentTimeMillis(); // We need unique sequence id
        transactionId = 0; // Tran id can be unique or per
    }

    public synchronized static SequenceId getNextSequenceId() {
        return new SequenceId(++sequenceId);
    }

    public synchronized static byte[] getNextTransactionId() {
        tranIdBuffer.clear();
        tranIdBuffer.putLong(++transactionId);
        return tranIdBuffer.array();
    }
}
