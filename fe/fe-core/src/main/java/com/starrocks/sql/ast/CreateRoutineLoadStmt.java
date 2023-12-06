// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.load.routineload.IcebergCreateRoutineLoadStmtConfig;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.LoadDataSourceType;
import com.starrocks.load.routineload.PulsarRoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/*
 Create routine Load statement,  continually load data from a streaming app

 syntax:
      CREATE ROUTINE LOAD [database.]name on table
      [load properties]
      [PROPERTIES
      (
          desired_concurrent_number = xxx,
          max_error_number = xxx,
          k1 = v1,
          ...
          kn = vn
      )]
      FROM type of routine load
      [(
          k1 = v1,
          ...
          kn = vn
      )]

      load properties:
          load property [[,] load property] ...

      load property:
          column separator | columns_mapping | partitions | where

      column separator:
          COLUMNS TERMINATED BY xxx
      columns_mapping:
          COLUMNS (c1, c2, c3 = c1 + c2)
      partitions:
          PARTITIONS (p1, p2, p3)
      where:
          WHERE c1 > 1

      type of routine load:
          KAFKA,
          PULSAR,
          TUBE,
          ICEBERG
*/
public class CreateRoutineLoadStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateRoutineLoadStmt.class);

    // routine load properties
    public static final String DESIRED_CONCURRENT_NUMBER_PROPERTY = "desired_concurrent_number";
    // max error number in ten thousand records
    public static final String MAX_ERROR_NUMBER_PROPERTY = "max_error_number";

    public static final String MAX_BATCH_INTERVAL_SEC_PROPERTY = "max_batch_interval";
    public static final String MAX_BATCH_ROWS_PROPERTY = "max_batch_rows";
    public static final String MAX_BATCH_SIZE_PROPERTY = "max_batch_size";  // deprecated

    public static final String RECOVER_OFFSETS_FROM_LAST_JOB = "recover_offsets_from_last_job";
    public static final String JOB_THAT_RECOVER_OFFSETS_FROM = "job_that_recover_offsets_from";

    // the value is csv or json, default is csv
    public static final String FORMAT = "format";
    public static final String STRIP_OUTER_ARRAY = "strip_outer_array";
    public static final String JSONPATHS = "jsonpaths";
    public static final String JSONROOT = "json_root";
    public static final String TIMEOUT_SECOND = "task_timeout_second";
    public static final String CONSUME_SECOND = "task_consume_second";

    // kafka type properties
    public static final String KAFKA_BROKER_LIST_PROPERTY = "kafka_broker_list";
    public static final String KAFKA_TOPIC_PROPERTY = "kafka_topic";
    // optional
    public static final String KAFKA_PARTITIONS_PROPERTY = "kafka_partitions";
    public static final String KAFKA_OFFSETS_PROPERTY = "kafka_offsets";
    public static final String KAFKA_AUTO_OFFSET_RESET_PROPERTY = "kafka_auto_offset_reset";
    public static final String KAFKA_DEFAULT_OFFSETS = "kafka_default_offsets";

    // pulsar type properties
    public static final String PULSAR_SERVICE_URL_PROPERTY = "pulsar_service_url";
    public static final String PULSAR_TOPIC_PROPERTY = "pulsar_topic";
    public static final String PULSAR_SUBSCRIPTION_PROPERTY = "pulsar_subscription";
    // optional
    public static final String PULSAR_PARTITIONS_PROPERTY = "pulsar_partitions";
    public static final String PULSAR_INITIAL_POSITIONS_PROPERTY = "pulsar_initial_positions";
    public static final String PULSAR_DEFAULT_INITIAL_POSITION = "pulsar_default_initial_position";
    public static final String PULSAR_AUTH_TOKEN = "auth.token";

    // tube type properties
    public static final String TUBE_MASTER_ADDR_PROPERTY = "tube_master_addr";
    public static final String TUBE_TOPIC_PROPERTY = "tube_topic";
    public static final String TUBE_GROUP_NAME_PROPERTY = "tube_group_name";
    public static final String TUBE_FILTERS_PROPERTY = "tube_tid";
    // optional
    public static final String TUBE_CONSUME_POSITION = "tube_consume_position";

    public static final String TUBE_FROM_FIRST = "FROM_FIRST"; // -1
    public static final String TUBE_FROM_LATEST = "FROM_LATEST"; // 0
    public static final String TUBE_FROM_MAX = "FROM_MAX"; // 1
    public static final int TUBE_FROM_FIRST_VAL = -1;
    public static final int TUBE_FROM_LATEST_VAL = 0;
    public static final int TUBE_FROM_MAX_VAL = 1;
    public static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(MAX_ERROR_NUMBER_PROPERTY)
            .add(MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(MAX_BATCH_ROWS_PROPERTY)
            .add(MAX_BATCH_SIZE_PROPERTY)
            .add(FORMAT)
            .add(JSONPATHS)
            .add(STRIP_OUTER_ARRAY)
            .add(TIMEOUT_SECOND)
            .add(CONSUME_SECOND)
            .add(JSONROOT)
            .add(RECOVER_OFFSETS_FROM_LAST_JOB)
            .add(JOB_THAT_RECOVER_OFFSETS_FROM)
            .add(LoadStmt.STRICT_MODE)
            .add(LoadStmt.IGNORE_TAIL_COLUMNS)
            .add(LoadStmt.SKIP_UTF8_CHECK)
            .add(LoadStmt.TASK_NUM_EXCEED_BE_NUM)
            .add(LoadStmt.TIMEZONE)

            .add(LoadStmt.PARTIAL_UPDATE)
            .add(LoadStmt.MERGE_CONDITION)
            .add(SessionVariable.EXEC_MEM_LIMIT)
            .add(SessionVariable.RESOURCE_GROUP)
            .build();

    private static final ImmutableSet<String> KAFKA_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(KAFKA_BROKER_LIST_PROPERTY)
            .add(KAFKA_TOPIC_PROPERTY)
            .add(KAFKA_PARTITIONS_PROPERTY)
            .add(KAFKA_OFFSETS_PROPERTY)
            .build();

    private static final ImmutableSet<String> PULSAR_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(PULSAR_SERVICE_URL_PROPERTY)
            .add(PULSAR_TOPIC_PROPERTY)
            .add(PULSAR_SUBSCRIPTION_PROPERTY)
            .add(PULSAR_PARTITIONS_PROPERTY)
            .add(PULSAR_INITIAL_POSITIONS_PROPERTY)
            .build();

    private static final ImmutableSet<String> TUBE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(TUBE_MASTER_ADDR_PROPERTY)
            .add(TUBE_TOPIC_PROPERTY)
            .add(TUBE_GROUP_NAME_PROPERTY)
            .add(TUBE_FILTERS_PROPERTY)
            .add(TUBE_CONSUME_POSITION)
            .build();

    private LabelName labelName;
    private final String tableName;
    private final List<ParseNode> loadPropertyList;
    private final Map<String, String> jobProperties;
    private final String typeName;
    private final Map<String, String> dataSourceProperties;

    // the following variables will be initialized after analyze
    // -1 as unset, the default value will set in RoutineLoadJob
    private String name;
    private String dbName;
    private RoutineLoadDesc routineLoadDesc;
    private int desiredConcurrentNum = 1;
    private long maxErrorNum = -1;
    private long maxBatchIntervalS = -1;
    private long maxBatchRows = -1;
    private boolean strictMode = true;
    private boolean ignoreTailColumns = false;
    private boolean skipUtf8Check = false;
    private boolean taskNumExceedBeNum = false;
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private String timeoutSecond = "0";
    private String consumeSecond = "0";
    private boolean partialUpdate = false;
    private boolean recoverOffsetsFromLastJob = false;
    private String jobThatRecoverOffsetsFrom;
    private String mergeConditionStr;
    private BrokerDesc brokerDesc;
    /**
     * RoutineLoad support json data.
     * Require Params:
     * 1) dataFormat = "json"
     * 2) jsonPaths = "$.XXX.xxx"
     */
    private String format = ""; //default is csv.
    private String jsonPaths = "";
    private String jsonRoot = ""; // MUST be a jsonpath string
    private boolean stripOuterArray = false;

    // kafka related properties
    private String kafkaBrokerList;
    private String kafkaTopic;
    // pair<partition id, offset>
    private List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();

    // custom kafka property map<key, value>
    private Map<String, String> customKafkaProperties = Maps.newHashMap();

    // pulsar related properties
    private String pulsarServiceUrl;
    private String pulsarTopic;
    private String pulsarSubscription;
    private List<Pair<String, MessageId>> pulsarPartitionInitialPositions = Lists.newArrayList();

    // custom pulsar property map<key, value>
    private Map<String, String> customPulsarProperties = Maps.newHashMap();

    // tube related properties
    private String tubeMasterAddr;
    private String tubeTopic;
    private String tubeGroupName;
    private String tubeFilters = null;
    private Integer tubeConsumePosition = null;

    private String resourceGroup = null;

    // iceberg related
    private IcebergCreateRoutineLoadStmtConfig icebergCreateRoutineLoadStmtConfig;

    public static final Predicate<Long> DESIRED_CONCURRENT_NUMBER_PRED = (v) -> v > 0L;
    public static final Predicate<Long> MAX_ERROR_NUMBER_PRED = (v) -> v >= 0L;
    public static final Predicate<Long> MAX_BATCH_INTERVAL_PRED = (v) -> v >= 5;
    public static final Predicate<Long> MAX_BATCH_ROWS_PRED = (v) -> v >= 200000;
    public static final Predicate<Long> TASK_TIMEOUT_SECOND_PRED = (v) -> v > 10L;
    public static final Predicate<Long> CONSUME_SECOND_PRED = (v) -> v > 5L;

    public CreateRoutineLoadStmt(LabelName labelName, String tableName, List<ParseNode> loadPropertyList,
                                 Map<String, String> jobProperties,
                                 String typeName, Map<String, String> dataSourceProperties, BrokerDesc brokerDesc) {
        this.labelName = labelName;
        this.tableName = tableName;
        this.loadPropertyList = loadPropertyList;
        this.jobProperties = jobProperties == null ? Maps.newHashMap() : jobProperties;
        this.typeName = typeName.toUpperCase();
        this.dataSourceProperties = dataSourceProperties;
        this.brokerDesc = brokerDesc;
    }

    public LabelName getLabelName() {
        return this.labelName;
    }

    public void setLabelName(LabelName labelName) {
        this.labelName = labelName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDBName() {
        return dbName;
    }

    public void setDBName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTypeName() {
        return typeName;
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }

    public void setRoutineLoadDesc(RoutineLoadDesc routineLoadDesc) {
        this.routineLoadDesc = routineLoadDesc;
    }

    public int getDesiredConcurrentNum() {
        return desiredConcurrentNum;
    }

    public long getMaxErrorNum() {
        return maxErrorNum;
    }

    public long getMaxBatchIntervalS() {
        return maxBatchIntervalS;
    }

    public long getMaxBatchRows() {
        return maxBatchRows;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public boolean isIgnoreTailColumns() {
        return ignoreTailColumns;
    }

    public boolean isSkipUtf8Check() {
        return skipUtf8Check;
    }

    public boolean isTaskNumExceedBeNum() {
        return taskNumExceedBeNum;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getTimeoutSecond() {
        return timeoutSecond;
    }

    public String getConsumeSecond() {
        return consumeSecond;
    }

    public boolean isPartialUpdate() {
        return partialUpdate;
    }

    public boolean isRecoverOffsetsFromLastJob() {
        return recoverOffsetsFromLastJob;
    }

    public String getJobThatRecoverOffsetsFrom() {
        return jobThatRecoverOffsetsFrom;
    }

    public String getMergeConditionStr() {
        return mergeConditionStr;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public String getFormat() {
        return format;
    }

    public boolean isStripOuterArray() {
        return stripOuterArray;
    }

    public String getJsonPaths() {
        return jsonPaths;
    }

    public String getJsonRoot() {
        return jsonRoot;
    }

    public String getKafkaBrokerList() {
        return kafkaBrokerList;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public List<Pair<Integer, Long>> getKafkaPartitionOffsets() {
        return kafkaPartitionOffsets;
    }

    public Map<String, String> getCustomKafkaProperties() {
        return customKafkaProperties;
    }

    public String getPulsarServiceUrl() {
        return pulsarServiceUrl;
    }

    public String getPulsarTopic() {
        return pulsarTopic;
    }

    public String getPulsarSubscription() {
        return pulsarSubscription;
    }

    public List<String> getPulsarPartitions() {
        return pulsarPartitionInitialPositions.stream().map(Pair::getFirst).collect(Collectors.toList());
    }

    public List<Pair<String, MessageId>> getPulsarPartitionInitialPositions() {
        return pulsarPartitionInitialPositions;
    }

    public Map<String, String> getCustomPulsarProperties() {
        return customPulsarProperties;
    }

    public String getTubeMasterAddr() {
        return tubeMasterAddr;
    }

    public String getTubeTopic() {
        return tubeTopic;
    }

    public String getTubeGroupName() {
        return tubeGroupName;
    }

    public String getTubeFilters() {
        return tubeFilters;
    }

    public Integer getTubeConsumePosition() {
        return tubeConsumePosition;
    }

    public IcebergCreateRoutineLoadStmtConfig getCreateIcebergRoutineLoadStmtConfig() {
        return icebergCreateRoutineLoadStmtConfig;
    }

    public List<ParseNode> getLoadPropertyList() {
        return loadPropertyList;
    }

    public final Map<String, String> getJobProperties() {
        return jobProperties;
    }

    public final Map<String, String> getDataSourceProperties() {
        return dataSourceProperties;
    }

    public String getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(String resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public static RoutineLoadDesc getLoadDesc(OriginStatement origStmt, Map<String, String> sessionVariables) {

        // parse the origin stmt to get routine load desc
        try {
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(
                    origStmt.originStmt, buildSessionVariables(sessionVariables));
            StatementBase stmt = stmts.get(origStmt.idx);
            if (stmt instanceof CreateRoutineLoadStmt) {
                return CreateRoutineLoadStmt.
                        buildLoadDesc(((CreateRoutineLoadStmt) stmt).getLoadPropertyList());
            } else if (stmt instanceof AlterRoutineLoadStmt) {
                return CreateRoutineLoadStmt.
                        buildLoadDesc(((AlterRoutineLoadStmt) stmt).getLoadPropertyList());
            } else {
                throw new IOException("stmt is neither CreateRoutineLoadStmt nor AlterRoutineLoadStmt");
            }
        } catch (Exception e) {
            LOG.error("error happens when parsing create/alter routine load stmt: " + origStmt.originStmt, e);
            return null;
        }
    }

    public static RoutineLoadDesc buildLoadDesc(List<ParseNode> loadPropertyList) throws UserException {
        if (loadPropertyList == null) {
            return null;
        }
        ColumnSeparator columnSeparator = null;
        RowDelimiter rowDelimiter = null;
        ImportColumnsStmt importColumnsStmt = null;
        ImportWhereStmt importWhereStmt = null;
        PartitionNames partitionNames = null;
        for (ParseNode parseNode : loadPropertyList) {
            if (parseNode instanceof ColumnSeparator) {
                // check column separator
                if (columnSeparator != null) {
                    throw new AnalysisException("repeat setting of column separator");
                }
                columnSeparator = (ColumnSeparator) parseNode;
            } else if (parseNode instanceof RowDelimiter) {
                // check row delimiter
                if (rowDelimiter != null) {
                    throw new AnalysisException("repeat setting of row delimiter");
                }
                rowDelimiter = (RowDelimiter) parseNode;
            } else if (parseNode instanceof ImportColumnsStmt) {
                // check columns info
                if (importColumnsStmt != null) {
                    throw new AnalysisException("repeat setting of columns info");
                }
                importColumnsStmt = (ImportColumnsStmt) parseNode;
            } else if (parseNode instanceof ImportWhereStmt) {
                // check where expr
                if (importWhereStmt != null) {
                    throw new AnalysisException("repeat setting of where predicate");
                }
                importWhereStmt = (ImportWhereStmt) parseNode;
            } else if (parseNode instanceof PartitionNames) {
                // check partition names
                if (partitionNames != null) {
                    throw new AnalysisException("repeat setting of partition names");
                }
                partitionNames = (PartitionNames) parseNode;
                partitionNames.analyze(null);
            }
        }
        return new RoutineLoadDesc(columnSeparator, rowDelimiter, importColumnsStmt, importWhereStmt,
                partitionNames);
    }

    public void checkJobProperties() throws UserException {
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        desiredConcurrentNum =
                ((Long) Util.getLongPropertyOrDefault(jobProperties.get(DESIRED_CONCURRENT_NUMBER_PROPERTY),
                        Config.max_routine_load_task_concurrent_num,
                        DESIRED_CONCURRENT_NUMBER_PRED,
                        DESIRED_CONCURRENT_NUMBER_PROPERTY + " should > 0")).intValue();

        maxErrorNum = Util.getLongPropertyOrDefault(jobProperties.get(MAX_ERROR_NUMBER_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_ERROR_NUM, MAX_ERROR_NUMBER_PRED,
                MAX_ERROR_NUMBER_PROPERTY + " should >= 0");

        maxBatchIntervalS = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_INTERVAL_SEC_PROPERTY),
                RoutineLoadJob.DEFAULT_TASK_SCHED_INTERVAL_SECOND, MAX_BATCH_INTERVAL_PRED,
                MAX_BATCH_INTERVAL_SEC_PROPERTY + " should >= 5");

        maxBatchRows = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_ROWS_PROPERTY),
                RoutineLoadJob.DEFAULT_MAX_BATCH_ROWS, MAX_BATCH_ROWS_PRED,
                MAX_BATCH_ROWS_PROPERTY + " should >= 200000");

        strictMode = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.STRICT_MODE),
                RoutineLoadJob.DEFAULT_STRICT_MODE,
                LoadStmt.STRICT_MODE + " should be a boolean");

        ignoreTailColumns = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.IGNORE_TAIL_COLUMNS),
                RoutineLoadJob.DEFAULT_IGNORE_TAIL_COLUMNS,
                LoadStmt.IGNORE_TAIL_COLUMNS + " should be a boolean");

        skipUtf8Check = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.SKIP_UTF8_CHECK),
                RoutineLoadJob.DEFAULT_SKIP_UTF8_CHECK,
                LoadStmt.SKIP_UTF8_CHECK + " should be a boolean");

        taskNumExceedBeNum = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.TASK_NUM_EXCEED_BE_NUM),
                RoutineLoadJob.DEFAULT_TASK_NUM_EXCEED_BE_NUM,
                LoadStmt.TASK_NUM_EXCEED_BE_NUM + " should be a boolean");

        partialUpdate = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.PARTIAL_UPDATE),
                false,
                LoadStmt.PARTIAL_UPDATE + " should be a boolean");

        recoverOffsetsFromLastJob = Util.getBooleanPropertyOrDefault(jobProperties.get(RECOVER_OFFSETS_FROM_LAST_JOB),
                false,
                LoadStmt.PARTIAL_UPDATE + " should be a boolean");
        resourceGroup = jobProperties.getOrDefault(SessionVariable.RESOURCE_GROUP, ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);

        jobThatRecoverOffsetsFrom = jobProperties.get(JOB_THAT_RECOVER_OFFSETS_FROM);

        mergeConditionStr = jobProperties.get(LoadStmt.MERGE_CONDITION);

        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        }
        timezone = TimeUtils.checkTimeZoneValidAndStandardize(jobProperties.getOrDefault(LoadStmt.TIMEZONE, timezone));

        long tmpTimeoutSecond = Util.getLongPropertyOrDefault(jobProperties.get(TIMEOUT_SECOND),
                Config.routine_load_task_timeout_second, TASK_TIMEOUT_SECOND_PRED,
                TIMEOUT_SECOND + " should > 10");

        long tmpConsumeSecond = Util.getLongPropertyOrDefault(jobProperties.get(CONSUME_SECOND),
                Config.routine_load_task_consume_second, CONSUME_SECOND_PRED, CONSUME_SECOND + " should > 5");

        if (tmpConsumeSecond >= tmpTimeoutSecond) {
            throw new AnalysisException(CONSUME_SECOND + " should <= " + TIMEOUT_SECOND);
        }

        // The following properties could be null, and we should use value from Config
        timeoutSecond = jobProperties.get(TIMEOUT_SECOND) == null ? "0" : jobProperties.get(TIMEOUT_SECOND);
        consumeSecond = jobProperties.get(CONSUME_SECOND) == null ? "0" : jobProperties.get(CONSUME_SECOND);

        format = jobProperties.get(FORMAT);
        if (format != null) {
            if (format.equalsIgnoreCase("csv")) {
                format = ""; // if it's not json, then it's mean csv and set empty
            } else if (format.equalsIgnoreCase("tdmsg_csv")) {
                format = "tdmsg_csv";
            } else if (format.equalsIgnoreCase("tdmsg_kv")) {
                format = "tdmsg_kv";
            } else if (format.equalsIgnoreCase("text_kv")) {
                format = "text_kv";
            } else if (format.equalsIgnoreCase("json")) {
                format = "json";
                jsonPaths = jobProperties.get(JSONPATHS);
                jsonRoot = jobProperties.get(JSONROOT);
                stripOuterArray = Boolean.valueOf(jobProperties.getOrDefault(STRIP_OUTER_ARRAY, "false"));
            } else {
                throw new UserException("Format type is invalid. format=`" + format + "`");
            }
        } else {
            format = "csv"; // default csv
        }
    }

    public void checkDataSourceProperties() throws AnalysisException {
        LoadDataSourceType type;
        try {
            type = LoadDataSourceType.valueOf(typeName);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("routine load job does not support this type " + typeName);
        }
        switch (type) {
            case KAFKA:
                checkKafkaProperties();
                break;
            case PULSAR:
                checkPulsarProperties();
                break;
            case TUBE:
                checkTubeProperties();
                break;
            case ICEBERG:
                icebergCreateRoutineLoadStmtConfig =
                        new IcebergCreateRoutineLoadStmtConfig(dataSourceProperties, brokerDesc);
                icebergCreateRoutineLoadStmtConfig.checkIcebergProperties();
                break;
            default:
                break;
        }
    }

    private void checkKafkaProperties() throws AnalysisException {
        Optional<String> optional = dataSourceProperties.keySet().stream()
                .filter(entity -> !KAFKA_PROPERTIES_SET.contains(entity))
                .filter(entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid kafka custom property");
        }

        // check broker list
        kafkaBrokerList = Strings.nullToEmpty(dataSourceProperties.get(KAFKA_BROKER_LIST_PROPERTY)).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(kafkaBrokerList)) {
            throw new AnalysisException(KAFKA_BROKER_LIST_PROPERTY + " is a required property");
        }
        String[] kafkaBrokerList = this.kafkaBrokerList.split(",");
        for (String broker : kafkaBrokerList) {
            if (!Pattern.matches(ENDPOINT_REGEX, broker)) {
                throw new AnalysisException(KAFKA_BROKER_LIST_PROPERTY + ":" + broker
                        + " not match pattern " + ENDPOINT_REGEX);
            }
        }

        // check topic
        kafkaTopic = Strings.nullToEmpty(dataSourceProperties.get(KAFKA_TOPIC_PROPERTY)).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(kafkaTopic)) {
            throw new AnalysisException(KAFKA_TOPIC_PROPERTY + " is a required property");
        }

        // check custom kafka property before check partitions,
        // because partitions can use kafka_default_offsets property
        analyzeKafkaCustomProperties(dataSourceProperties, customKafkaProperties);

        // check partitions
        String kafkaPartitionsString = dataSourceProperties.get(KAFKA_PARTITIONS_PROPERTY);
        if (kafkaPartitionsString != null) {
            analyzeKafkaPartitionProperty(kafkaPartitionsString, customKafkaProperties, kafkaPartitionOffsets);
        }

        // check offset
        String kafkaOffsetsString = dataSourceProperties.get(KAFKA_OFFSETS_PROPERTY);
        if (kafkaOffsetsString != null) {
            analyzeKafkaOffsetProperty(kafkaOffsetsString, kafkaPartitionOffsets);
        }
    }

    public static void analyzeKafkaPartitionProperty(String kafkaPartitionsString,
                                                     Map<String, String> customKafkaProperties,
                                                     List<Pair<Integer, Long>> kafkaPartitionOffsets)
            throws AnalysisException {
        kafkaPartitionsString = kafkaPartitionsString.replaceAll(" ", "");
        if (kafkaPartitionsString.isEmpty()) {
            throw new AnalysisException(KAFKA_PARTITIONS_PROPERTY + " could not be a empty string");
        }

        // get kafka default offset if set
        Long kafkaDefaultOffset = null;
        if (customKafkaProperties.containsKey(KAFKA_DEFAULT_OFFSETS)) {
            kafkaDefaultOffset = getKafkaOffset(customKafkaProperties.get(KAFKA_DEFAULT_OFFSETS));
        }

        String[] kafkaPartitionsStringList = kafkaPartitionsString.split(",");
        for (String s : kafkaPartitionsStringList) {
            try {
                kafkaPartitionOffsets.add(
                        Pair.create(getIntegerValueFromString(s, KAFKA_PARTITIONS_PROPERTY),
                                kafkaDefaultOffset == null ? KafkaProgress.OFFSET_END_VAL : kafkaDefaultOffset));
            } catch (AnalysisException e) {
                throw new AnalysisException(KAFKA_PARTITIONS_PROPERTY
                        + " must be a number string with comma-separated");
            }
        }
    }

    public static void analyzeKafkaOffsetProperty(String kafkaOffsetsString,
                                                  List<Pair<Integer, Long>> kafkaPartitionOffsets)
            throws AnalysisException {
        kafkaOffsetsString = kafkaOffsetsString.replaceAll(" ", "");
        if (kafkaOffsetsString.isEmpty()) {
            throw new AnalysisException(KAFKA_OFFSETS_PROPERTY + " could not be a empty string");
        }
        String[] kafkaOffsetsStringList = kafkaOffsetsString.split(",");
        if (kafkaOffsetsStringList.length != kafkaPartitionOffsets.size()) {
            throw new AnalysisException("Partitions number should be equals to offsets number");
        }

        for (int i = 0; i < kafkaOffsetsStringList.length; i++) {
            kafkaPartitionOffsets.get(i).second = getKafkaOffset(kafkaOffsetsStringList[i]);
        }
    }

    // Get kafka offset from string
    // defined in librdkafka/rdkafkacpp.h
    // OFFSET_BEGINNING: -2
    // OFFSET_END: -1
    public static long getKafkaOffset(String offsetStr) throws AnalysisException {
        long offset = -1;
        try {
            offset = getLongValueFromString(offsetStr, "kafka offset");
            if (offset < 0) {
                throw new AnalysisException("Can not specify offset smaller than 0");
            }
        } catch (AnalysisException e) {
            if (offsetStr.equalsIgnoreCase(KafkaProgress.OFFSET_BEGINNING)) {
                offset = KafkaProgress.OFFSET_BEGINNING_VAL;
            } else if (offsetStr.equalsIgnoreCase(KafkaProgress.OFFSET_END)) {
                offset = KafkaProgress.OFFSET_END_VAL;
            } else {
                throw e;
            }
        }
        return offset;
    }

    public static void setCustomProperties(Map<String, String> dataSourceProperties,
                                           Map<String, String> customProperties) throws AnalysisException {
        for (Map.Entry<String, String> dataSourceProperty : dataSourceProperties.entrySet()) {
            if (dataSourceProperty.getKey().startsWith("property.")) {
                String propertyKey = dataSourceProperty.getKey();
                String propertyValue = dataSourceProperty.getValue();
                String[] propertyValueArr = propertyKey.split("\\.");
                if (propertyValueArr.length < 2) {
                    throw new AnalysisException("property value could not be a empty string");
                }
                customProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
            }
            // can be extended in the future which other prefix
        }
    }

    public static void analyzeKafkaCustomProperties(Map<String, String> dataSourceProperties,
                                                    Map<String, String> customKafkaProperties)
            throws AnalysisException {
        setCustomProperties(dataSourceProperties, customKafkaProperties);

        // check kafka_default_offsets
        if (customKafkaProperties.containsKey(KAFKA_DEFAULT_OFFSETS)) {
            getKafkaOffset(customKafkaProperties.get(KAFKA_DEFAULT_OFFSETS));
        }

        // check auto_offset_reset
        Util.getBooleanPropertyOrDefault(customKafkaProperties.get(KAFKA_AUTO_OFFSET_RESET_PROPERTY),
                false,
                KAFKA_AUTO_OFFSET_RESET_PROPERTY + " should be a boolean");
    }

    private void checkPulsarProperties() throws AnalysisException {
        Optional<String> optional = dataSourceProperties.keySet().stream()
                .filter(entity -> !PULSAR_PROPERTIES_SET.contains(entity))
                .filter(entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid pulsar custom property");
        }

        // check service url
        pulsarServiceUrl =
                Strings.nullToEmpty(dataSourceProperties.get(PULSAR_SERVICE_URL_PROPERTY)).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(pulsarServiceUrl)) {
            throw new AnalysisException(PULSAR_SERVICE_URL_PROPERTY + " is a required property");
        }

        String[] pulsarServiceUrlList = this.pulsarServiceUrl.split(",");
        for (String serviceUrl : pulsarServiceUrlList) {
            if (!Pattern.matches(ENDPOINT_REGEX, serviceUrl)) {
                throw new AnalysisException(PULSAR_SERVICE_URL_PROPERTY + ":" + serviceUrl
                        + " not match pattern " + ENDPOINT_REGEX);
            }
        }

        // check topic
        pulsarTopic = Strings.nullToEmpty(dataSourceProperties.get(PULSAR_TOPIC_PROPERTY)).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(pulsarTopic)) {
            throw new AnalysisException(PULSAR_TOPIC_PROPERTY + " is a required property");
        }

        // check subscription
        pulsarSubscription =
                Strings.nullToEmpty(dataSourceProperties.get(PULSAR_SUBSCRIPTION_PROPERTY)).replaceAll(" ", "");

        // check custom pulsar property before check partitions,
        // because partitions can use pulsar_default_position property
        analyzePulsarCustomProperties(dataSourceProperties, customPulsarProperties);

        // check partitions
        String pulsarPartitionsString = dataSourceProperties.get(PULSAR_PARTITIONS_PROPERTY);
        if (pulsarPartitionsString != null) {
            analyzePulsarPartitionProperty(pulsarPartitionsString, customPulsarProperties,
                    pulsarPartitionInitialPositions);
        }

        // check positions
        String pulsarPositionString = dataSourceProperties.get(PULSAR_INITIAL_POSITIONS_PROPERTY);
        if (pulsarPositionString != null) {
            analyzePulsarPositionProperty(pulsarPositionString, pulsarPartitionInitialPositions);
        }
    }

    public static void analyzePulsarPartitionProperty(String pulsarPartitionsString,
                                                      Map<String, String> customPulsarProperties,
                                                      List<Pair<String, MessageId>> pulsarPartitionInitialPositions)
            throws AnalysisException {
        pulsarPartitionsString = pulsarPartitionsString.replaceAll(" ", "");
        if (pulsarPartitionsString.isEmpty()) {
            throw new AnalysisException(PULSAR_PARTITIONS_PROPERTY + " could not be a empty string");
        }

        // get pulsar default initial position if set
        MessageId pulsarDefaultInitialPosition = null;
        if (customPulsarProperties.containsKey(PULSAR_DEFAULT_INITIAL_POSITION)) {
            pulsarDefaultInitialPosition =
                    getPulsarPosition(customPulsarProperties.get(PULSAR_DEFAULT_INITIAL_POSITION));
        }

        String[] pulsarPartitionsStringList = pulsarPartitionsString.split(",");
        for (String s : pulsarPartitionsStringList) {
            pulsarPartitionInitialPositions.add(
                    Pair.create(s,
                            pulsarDefaultInitialPosition == null ? MessageId.latest : pulsarDefaultInitialPosition));
        }
    }

    public static void analyzePulsarPositionProperty(String pulsarPositionsString,
                                                     List<Pair<String, MessageId>> pulsarPartitionInitialPositions)
            throws AnalysisException {
        pulsarPositionsString = pulsarPositionsString.replaceAll(" ", "");
        if (pulsarPositionsString.isEmpty()) {
            throw new AnalysisException(PULSAR_INITIAL_POSITIONS_PROPERTY + " could not be a empty string");
        }
        String[] pulsarPositionsStringList = pulsarPositionsString.split(",");
        if (pulsarPositionsStringList.length != pulsarPartitionInitialPositions.size()) {
            throw new AnalysisException("Partitions number should be equals to positions number");
        }

        for (int i = 0; i < pulsarPositionsStringList.length; i++) {
            pulsarPartitionInitialPositions.get(i).second = getPulsarPosition(pulsarPositionsStringList[i]);
        }
    }

    // Get pulsar position from string
    // defined in pulsar-client-cpp/InitialPosition.h
    // InitialPositionLatest: 0
    // InitialPositionEarliest: 1
    public static MessageId getPulsarPosition(String positionStr) throws AnalysisException {
        MessageId position;
        if (positionStr.equalsIgnoreCase(PulsarRoutineLoadJob.POSITION_EARLIEST)) {
            position = MessageId.earliest;
        } else if (positionStr.equalsIgnoreCase(PulsarRoutineLoadJob.POSITION_LATEST)) {
            position = MessageId.latest;
        } else {
            throw new AnalysisException("Only POSITION_EARLIEST or POSITION_LATEST can be specified");
        }
        return position;
    }

    public static void analyzePulsarCustomProperties(Map<String, String> dataSourceProperties,
                                                     Map<String, String> customPulsarProperties)
            throws AnalysisException {
        setCustomProperties(dataSourceProperties, customPulsarProperties);
    }

    private void checkTubeProperties() throws AnalysisException {
        Optional<String> optional = dataSourceProperties.keySet().stream()
                .filter(entity -> !TUBE_PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid tube custom property");
        }

        // check tube master address
        tubeMasterAddr = Strings.nullToEmpty(dataSourceProperties.get(TUBE_MASTER_ADDR_PROPERTY)).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(tubeMasterAddr)) {
            throw new AnalysisException(TUBE_MASTER_ADDR_PROPERTY + " is a required property");
        }

        if (!Pattern.matches(ENDPOINT_REGEX, tubeMasterAddr)) {
            throw new AnalysisException(TUBE_MASTER_ADDR_PROPERTY + ":" + tubeMasterAddr
                    + " not match pattern " + ENDPOINT_REGEX);
        }

        // check topic
        tubeTopic = Strings.nullToEmpty(dataSourceProperties.get(TUBE_TOPIC_PROPERTY)).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(tubeTopic)) {
            throw new AnalysisException(TUBE_TOPIC_PROPERTY + " is a required property");
        }

        // check group name
        tubeGroupName = Strings.nullToEmpty(dataSourceProperties.get(TUBE_GROUP_NAME_PROPERTY)).replaceAll(" ", "");
        if (Strings.isNullOrEmpty(tubeGroupName)) {
            throw new AnalysisException(TUBE_GROUP_NAME_PROPERTY + " is a required property");
        }

        // check filters
        String filters = dataSourceProperties.get(TUBE_FILTERS_PROPERTY);
        if (filters != null) {
            tubeFilters = filters;
        }

        // check positions
        String consumePosition = dataSourceProperties.get(TUBE_CONSUME_POSITION);
        if (consumePosition != null) {
            tubeConsumePosition = getTubeConsumePosition(consumePosition);
        }
    }

    public static int getTubeConsumePosition(String consumePositionStr) throws AnalysisException {
        int position;
        if (consumePositionStr.equalsIgnoreCase(TUBE_FROM_FIRST)) {
            position = TUBE_FROM_FIRST_VAL;
        } else if (consumePositionStr.equalsIgnoreCase(TUBE_FROM_LATEST)) {
            position = TUBE_FROM_LATEST_VAL;
        } else if (consumePositionStr.equalsIgnoreCase(TUBE_FROM_MAX)) {
            position = TUBE_FROM_MAX_VAL;
        } else {
            throw new AnalysisException("Only FROM_FIRST/FROM_LATEST/FROM_MAX can be specified");
        }
        return position;
    }

    private static int getIntegerValueFromString(String valueString, String propertyName) throws AnalysisException {
        if (valueString.isEmpty()) {
            throw new AnalysisException(propertyName + " could not be a empty string");
        }
        int value;
        try {
            value = Integer.parseInt(valueString);
        } catch (NumberFormatException e) {
            throw new AnalysisException(propertyName + " must be a integer");
        }
        return value;
    }

    private static long getLongValueFromString(String valueString, String propertyName) throws AnalysisException {
        if (valueString.isEmpty()) {
            throw new AnalysisException(propertyName + " could not be a empty string");
        }
        long value;
        try {
            value = Long.valueOf(valueString);
        } catch (NumberFormatException e) {
            throw new AnalysisException(propertyName + " must be a integer: " + valueString);
        }
        return value;
    }

    private static SessionVariable buildSessionVariables(Map<String, String> sessionVariables) {
        SessionVariable sessionVariable = new SessionVariable();
        if (sessionVariables == null) {
            return sessionVariable;
        }
        if (sessionVariables.containsKey(SessionVariable.SQL_MODE)) {
            sessionVariable.setSqlMode(Long.parseLong(sessionVariables.get(SessionVariable.SQL_MODE)));
        }

        if (sessionVariables.containsKey(SessionVariable.PARSE_TOKENS_LIMIT)) {
            sessionVariable.setParseTokensLimit(Integer.parseInt(
                    sessionVariables.get(SessionVariable.PARSE_TOKENS_LIMIT)));
        }
        return sessionVariable;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws RuntimeException {
        return visitor.visitCreateRoutineLoadStatement(this, context);
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }
}
