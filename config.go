package config

import (
	"log"
	"math"
	"strings"
	"time"


	"github.com/guygma/goKCL/utils"
	"github.com/aws/aws-sdk-go/aws"
	creds "github.com/aws/aws-sdk-go/aws/credentials"
)

const (
	// LATEST start after the most recent data record (fetch new data).
	LATEST InitialPositionInStream = iota + 1
	// TRIM_HORIZON start from the oldest available data record
	TRIM_HORIZON
	// AT_TIMESTAMP start from the record at or after the specified server-side Timestamp.
	AT_TIMESTAMP

	// The location in the shard from which the KinesisClientLibrary will start fetching record from
	// when the application starts for the first time and there is no checkpoint for the shard.
	DEFAULT_INITIAL_POSITION_IN_STREAM = LATEST

	// Fail over time in milliseconds. A worker which does not renew it's lease within this time interval
	// will be regarded as having problems and it's shards will be assigned to other workers.
	// For applications that have a large number of shards, this may be set to a higher number to reduce
	// the number of DynamoDB IOPS required for tracking leases.
	DEFAULT_FAILOVER_TIME_MILLIS = 10000

	// Max record to fetch from Kinesis in a single GetRecords call.
	DEFAULT_MAX_RECORDS = 10000

	// The default value for how long the {@link ShardConsumer} should sleep if no record are returned
	// from the call to
	DEFAULT_IDLETIME_BETWEEN_READS_MILLIS = 1000

	// Don't call processRecords() on the record processor for empty record lists.
	DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST = false

	// Interval in milliseconds between polling to check for parent shard completion.
	// Polling frequently will take up more DynamoDB IOPS (when there are leases for shards waiting on
	// completion of parent shards).
	DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS = 10000

	// Shard sync interval in milliseconds - e.g. wait for this long between shard sync tasks.
	DEFAULT_SHARD_SYNC_INTERVAL_MILLIS = 60000

	// Cleanup leases upon shards completion (don't wait until they expire in Kinesis).
	// Keeping leases takes some tracking/resources (e.g. they need to be renewed, assigned), so by
	// default we try to delete the ones we don't need any longer.
	DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION = true

	// Backoff time in milliseconds for Amazon Kinesis Client Library tasks (in the event of failures).
	DEFAULT_TASK_BACKOFF_TIME_MILLIS = 500

	// Buffer metrics for at most this long before publishing to CloudWatch.
	DEFAULT_METRICS_BUFFER_TIME_MILLIS = 10000

	// Buffer at most this many metrics before publishing to CloudWatch.
	DEFAULT_METRICS_MAX_QUEUE_SIZE = 10000

	// KCL will validate client provided sequence numbers with a call to Amazon Kinesis before
	// checkpointing for calls to {@link RecordProcessorCheckpointer#checkpoint(String)} by default.
	DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING = true

	// The max number of leases (shards) this worker should process.
	// This can be useful to avoid overloading (and thrashing) a worker when a host has resource constraints
	// or during deployment.
	// NOTE: Setting this to a low value can cause data loss if workers are not able to pick up all shards in the
	// stream due to the max limit.
	DEFAULT_MAX_LEASES_FOR_WORKER = math.MaxInt16

	// Max leases to steal from another worker at one time (for load balancing).
	// Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
	// but can cause higher churn in the system.
	DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1

	// The Amazon DynamoDB table used for tracking leases will be provisioned with this read capacity.
	DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY = 10

	// The Amazon DynamoDB table used for tracking leases will be provisioned with this write capacity.
	DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10

	// The Worker will skip shard sync during initialization if there are one or more leases in the lease table. This
	// assumes that the shards and leases are in-sync. This enables customers to choose faster startup times (e.g.
	// during incremental deployments of an application).
	DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST = false

	// The amount of milliseconds to wait before graceful shutdown forcefully terminates.
	DEFAULT_SHUTDOWN_GRACE_MILLIS = 5000

	// The size of the thread pool to create for the lease renewer to use.
	DEFAULT_MAX_LEASE_RENEWAL_THREADS = 20

	// The sleep time between two listShards calls from the proxy when throttled.
	DEFAULT_LIST_SHARDS_BACKOFF_TIME_IN_MILLIS = 1500

	// The number of times the Proxy will retry listShards call when throttled.
	DEFAULT_MAX_LIST_SHARDS_RETRY_ATTEMPTS = 50
)

type (
	// InitialPositionInStream Used to specify the Position in the stream where a new application should start from
	// This is used during initial application bootstrap (when a checkpoint doesn't exist for a shard or its parents)
	InitialPositionInStream int

	// Class that houses the entities needed to specify the Position in the stream from where a new application should
	// start.
	InitialPositionInStreamExtended struct {
		Position InitialPositionInStream

		// The time stamp of the data record from which to start reading. Used with
		// shard iterator type AT_TIMESTAMP. A time stamp is the Unix epoch date with
		// precision in milliseconds. For example, 2016-04-04T19:58:46.480-00:00 or
		// 1459799926.480. If a record with this exact time stamp does not exist, the
		// iterator returned is for the next (later) record. If the time stamp is older
		// than the current trim horizon, the iterator returned is for the oldest untrimmed
		// data record (TRIM_HORIZON).
		Timestamp *time.Time `type:"Timestamp" timestampFormat:"unix"`
	}

	// Configuration for the Kinesis Client Library.
	// Note: There is no need to configure credential provider. Credential can be get from InstanceProfile.
	KinesisClientLibConfiguration struct {
		// ApplicationName is name of application. Kinesis allows multiple applications to consume the same stream.
		ApplicationName string

		// DynamoDBEndpoint is an optional endpoint URL that overrides the default generated endpoint for a DynamoDB client.
		// If this is empty, the default generated endpoint will be used.
		DynamoDBEndpoint string

		// KinesisEndpoint is an optional endpoint URL that overrides the default generated endpoint for a Kinesis client.
		// If this is empty, the default generated endpoint will be used.
		KinesisEndpoint string

		// KinesisCredentials is used to access Kinesis
		KinesisCredentials *creds.Credentials

		// DynamoDBCredentials is used to access DynamoDB
		DynamoDBCredentials *creds.Credentials

		// CloudWatchCredentials is used to access CloudWatch
		CloudWatchCredentials *creds.Credentials

		// TableName is name of the dynamo db table for managing kinesis stream default to ApplicationName
		TableName string

		// StreamName is the name of Kinesis stream
		StreamName string

		// WorkerID used to distinguish different workers/processes of a Kinesis application
		WorkerID string

		// InitialPositionInStream specifies the Position in the stream where a new application should start from
		InitialPositionInStream InitialPositionInStream

		// InitialPositionInStreamExtended provides actual AT_TMESTAMP value
		InitialPositionInStreamExtended InitialPositionInStreamExtended

		// credentials to access Kinesis/Dynamo/CloudWatch: https://docs.aws.amazon.com/sdk-for-go/api/aws/credentials/
		// Note: No need to configure here. Use NewEnvCredentials for testing and EC2RoleProvider for production

		// FailoverTimeMillis Lease duration (leases not renewed within this period will be claimed by others)
		FailoverTimeMillis int

		/// MaxRecords Max record to read per Kinesis getRecords() call
		MaxRecords int

		// IdleTimeBetweenReadsInMillis Idle time between calls to fetch data from Kinesis
		IdleTimeBetweenReadsInMillis int

		// CallProcessRecordsEvenForEmptyRecordList Call the IRecordProcessor::processRecords() API even if
		// GetRecords returned an empty record list.
		CallProcessRecordsEvenForEmptyRecordList bool

		// ParentShardPollIntervalMillis Wait for this long between polls to check if parent shards are done
		ParentShardPollIntervalMillis int

		// ShardSyncIntervalMillis Time between tasks to sync leases and Kinesis shards
		ShardSyncIntervalMillis int

		// CleanupTerminatedShardsBeforeExpiry Clean up shards we've finished processing (don't wait for expiration)
		CleanupTerminatedShardsBeforeExpiry bool

		// kinesisClientConfig Client Configuration used by Kinesis client
		// dynamoDBClientConfig Client Configuration used by DynamoDB client
		// cloudWatchClientConfig Client Configuration used by CloudWatch client
		// Note: we will use default client provided by AWS SDK

		// TaskBackoffTimeMillis Backoff period when tasks encounter an exception
		TaskBackoffTimeMillis int

		// MetricsBufferTimeMillis Metrics are buffered for at most this long before publishing to CloudWatch
		MetricsBufferTimeMillis int

		// MetricsMaxQueueSize Max number of metrics to buffer before publishing to CloudWatch
		MetricsMaxQueueSize int

		// ValidateSequenceNumberBeforeCheckpointing whether KCL should validate client provided sequence numbers
		ValidateSequenceNumberBeforeCheckpointing bool

		// RegionName The region name for the service
		RegionName string

		// ShutdownGraceMillis The number of milliseconds before graceful shutdown terminates forcefully
		ShutdownGraceMillis int

		// Operation parameters

		// Max leases this Worker can handle at a time
		MaxLeasesForWorker int

		// Max leases to steal at one time (for load balancing)
		MaxLeasesToStealAtOneTime int

		// Read capacity to provision when creating the lease table (dynamoDB).
		InitialLeaseTableReadCapacity int

		// Write capacity to provision when creating the lease table.
		InitialLeaseTableWriteCapacity int

		// Worker should skip syncing shards and leases at startup if leases are present
		// This is useful for optimizing deployments to large fleets working on a stable stream.
		SkipShardSyncAtWorkerInitializationIfLeasesExist bool
	}
)

var positionMap = map[InitialPositionInStream]*string{
	LATEST:       aws.String("LATEST"),
	TRIM_HORIZON: aws.String("TRIM_HORIZON"),
	AT_TIMESTAMP: aws.String("AT_TIMESTAMP"),
}

func InitalPositionInStreamToShardIteratorType(pos InitialPositionInStream) *string {
	return positionMap[pos]
}

func empty(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

// checkIsValuePositive make sure the value is possitive.
func checkIsValueNotEmpty(key string, value string) {
	if empty(value) {
		// There is no point to continue for incorrect configuration. Fail fast!
		log.Panicf("Non-empty value exepected for %v, actual: %v", key, value)
	}
}

// checkIsValuePositive make sure the value is possitive.
func checkIsValuePositive(key string, value int) {
	if value <= 0 {
		// There is no point to continue for incorrect configuration. Fail fast!
		log.Panicf("Positive value exepected for %v, actual: %v", key, value)
	}
}


func newInitialPositionAtTimestamp(timestamp *time.Time) *InitialPositionInStreamExtended {
	return &InitialPositionInStreamExtended{Position: AT_TIMESTAMP, Timestamp: timestamp}
}

func newInitialPosition(position InitialPositionInStream) *InitialPositionInStreamExtended {
	return &InitialPositionInStreamExtended{Position: position, Timestamp: nil}
}


// NewKinesisClientLibConfig to create a default KinesisClientLibConfiguration based on the required fields.
func NewKinesisClientLibConfig(applicationName, streamName, regionName, workerID string) *KinesisClientLibConfiguration {
	return NewKinesisClientLibConfigWithCredentials(applicationName, streamName, regionName, workerID,
		nil, nil, nil)
}

// NewKinesisClientLibConfig to create a default KinesisClientLibConfiguration based on the required fields.
func NewKinesisClientLibConfigWithCredential(applicationName, streamName, regionName, workerID string,
	creds *credentials.Credentials) *KinesisClientLibConfiguration {
	return NewKinesisClientLibConfigWithCredentials(applicationName, streamName, regionName, workerID, creds, creds, creds)
}

// NewKinesisClientLibConfig to create a default KinesisClientLibConfiguration based on the required fields.
func NewKinesisClientLibConfigWithCredentials(applicationName, streamName, regionName, workerID string,
	kiniesisCreds, dynamodbCreds, cloudwatchCreds *credentials.Credentials) *KinesisClientLibConfiguration {
	checkIsValueNotEmpty("ApplicationName", applicationName)
	checkIsValueNotEmpty("StreamName", streamName)
	checkIsValueNotEmpty("RegionName", regionName)

	if empty(workerID) {
		workerID = utils.MustNewUUID()
	}

	// populate the KCL configuration with default values
	return &KinesisClientLibConfiguration{
		ApplicationName:                                  applicationName,
		KinesisCredentials:                               kiniesisCreds,
		DynamoDBCredentials:                              dynamodbCreds,
		CloudWatchCredentials:                            cloudwatchCreds,
		TableName:                                        applicationName,
		StreamName:                                       streamName,
		RegionName:                                       regionName,
		WorkerID:                                         workerID,
		InitialPositionInStream:                          DEFAULT_INITIAL_POSITION_IN_STREAM,
		InitialPositionInStreamExtended:                  *newInitialPosition(DEFAULT_INITIAL_POSITION_IN_STREAM),
		FailoverTimeMillis:                               DEFAULT_FAILOVER_TIME_MILLIS,
		MaxRecords:                                       DEFAULT_MAX_RECORDS,
		IdleTimeBetweenReadsInMillis:                     DEFAULT_IDLETIME_BETWEEN_READS_MILLIS,
		CallProcessRecordsEvenForEmptyRecordList:         DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST,
		ParentShardPollIntervalMillis:                    DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS,
		ShardSyncIntervalMillis:                          DEFAULT_SHARD_SYNC_INTERVAL_MILLIS,
		CleanupTerminatedShardsBeforeExpiry:              DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
		TaskBackoffTimeMillis:                            DEFAULT_TASK_BACKOFF_TIME_MILLIS,
		MetricsBufferTimeMillis:                          DEFAULT_METRICS_BUFFER_TIME_MILLIS,
		MetricsMaxQueueSize:                              DEFAULT_METRICS_MAX_QUEUE_SIZE,
		ValidateSequenceNumberBeforeCheckpointing:        DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING,
		ShutdownGraceMillis:                              DEFAULT_SHUTDOWN_GRACE_MILLIS,
		MaxLeasesForWorker:                               DEFAULT_MAX_LEASES_FOR_WORKER,
		MaxLeasesToStealAtOneTime:                        DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME,
		InitialLeaseTableReadCapacity:                    DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY,
		InitialLeaseTableWriteCapacity:                   DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY,
		SkipShardSyncAtWorkerInitializationIfLeasesExist: DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
	}
}

// WithKinesisEndpoint is used to provide an alternative Kinesis endpoint
func (c *KinesisClientLibConfiguration) WithKinesisEndpoint(kinesisEndpoint string) *KinesisClientLibConfiguration {
	c.KinesisEndpoint = kinesisEndpoint
	return c
}

// WithDynamoDBEndpoint is used to provide an alternative DynamoDB endpoint
func (c *KinesisClientLibConfiguration) WithDynamoDBEndpoint(dynamoDBEndpoint string) *KinesisClientLibConfiguration {
	c.DynamoDBEndpoint = dynamoDBEndpoint
	return c
}

// WithTableName to provide alternative lease table in DynamoDB
func (c *KinesisClientLibConfiguration) WithTableName(tableName string) *KinesisClientLibConfiguration {
	c.TableName = tableName
	return c
}

func (c *KinesisClientLibConfiguration) WithInitialPositionInStream(initialPositionInStream InitialPositionInStream) *KinesisClientLibConfiguration {
	c.InitialPositionInStream = initialPositionInStream
	c.InitialPositionInStreamExtended = *newInitialPosition(initialPositionInStream)
	return c
}

func (c *KinesisClientLibConfiguration) WithTimestampAtInitialPositionInStream(timestamp *time.Time) *KinesisClientLibConfiguration {
	c.InitialPositionInStream = AT_TIMESTAMP
	c.InitialPositionInStreamExtended = *newInitialPositionAtTimestamp(timestamp)
	return c
}

func (c *KinesisClientLibConfiguration) WithFailoverTimeMillis(failoverTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("FailoverTimeMillis", failoverTimeMillis)
	c.FailoverTimeMillis = failoverTimeMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithShardSyncIntervalMillis(shardSyncIntervalMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("ShardSyncIntervalMillis", shardSyncIntervalMillis)
	c.ShardSyncIntervalMillis = shardSyncIntervalMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithMaxRecords(maxRecords int) *KinesisClientLibConfiguration {
	checkIsValuePositive("MaxRecords", maxRecords)
	c.MaxRecords = maxRecords
	return c
}

// WithMaxLeasesForWorker configures maximum lease this worker can handles. It determines how maximun number of shards
// this worker can handle.
func (c *KinesisClientLibConfiguration) WithMaxLeasesForWorker(n int) *KinesisClientLibConfiguration {
	checkIsValuePositive("MaxLeasesForWorker", n)
	c.MaxLeasesForWorker = n
	return c
}

/**
 * Controls how long the KCL will sleep if no record are returned from Kinesis
 *
 * <p>
 * This value is only used when no record are returned; if record are returned, the {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ProcessTask} will
 * immediately retrieve the next set of record after the call to
 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#processRecords(ProcessRecordsInput)}
 * has returned. Setting this value to high may result in the KCL being unable to catch up. If you are changing this
 * value it's recommended that you enable {@link #withCallProcessRecordsEvenForEmptyRecordList(boolean)}, and
 * monitor how far behind the record retrieved are by inspecting
 * {@link com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput#getMillisBehindLatest()}, and the
 * <a href=
 * "http://docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html#kinesis-metrics-stream">CloudWatch
 * Metric: GetRecords.MillisBehindLatest</a>
 * </p>
 *
 * @param IdleTimeBetweenReadsInMillis
 *            how long to sleep between GetRecords calls when no record are returned.
 * @return KinesisClientLibConfiguration
 */
func (c *KinesisClientLibConfiguration) WithIdleTimeBetweenReadsInMillis(idleTimeBetweenReadsInMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("IdleTimeBetweenReadsInMillis", idleTimeBetweenReadsInMillis)
	c.IdleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithCallProcessRecordsEvenForEmptyRecordList(callProcessRecordsEvenForEmptyRecordList bool) *KinesisClientLibConfiguration {
	c.CallProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList
	return c
}

func (c *KinesisClientLibConfiguration) WithTaskBackoffTimeMillis(taskBackoffTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("TaskBackoffTimeMillis", taskBackoffTimeMillis)
	c.TaskBackoffTimeMillis = taskBackoffTimeMillis
	return c
}

// WithMetricsBufferTimeMillis configures Metrics are buffered for at most this long before publishing to CloudWatch
func (c *KinesisClientLibConfiguration) WithMetricsBufferTimeMillis(metricsBufferTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("MetricsBufferTimeMillis", metricsBufferTimeMillis)
	c.MetricsBufferTimeMillis = metricsBufferTimeMillis
	return c
}

// WithMetricsMaxQueueSize configures Max number of metrics to buffer before publishing to CloudWatch
func (c *KinesisClientLibConfiguration) WithMetricsMaxQueueSize(metricsMaxQueueSize int) *KinesisClientLibConfiguration {
	checkIsValuePositive("MetricsMaxQueueSize", metricsMaxQueueSize)
	c.MetricsMaxQueueSize = metricsMaxQueueSize
	return c
}
