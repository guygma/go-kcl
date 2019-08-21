package shard

import (
	"github.com/guygma/goKCL/record"
	log "github.com/sirupsen/logrus"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/guygma/goKCL"
	"github.com/guygma/goKCL/util"
)

const (
	// This is the initial state of a shard consumer. This causes the consumer to remain blocked until the all
	// parent shards have been completed.
	WAITING_ON_PARENT_SHARDS ConsumerState = iota + 1

	// This state is responsible for initializing the record processor with the shard information.
	INITIALIZING

	//
	PROCESSING

	SHUTDOWN_REQUESTED

	SHUTTING_DOWN

	SHUTDOWN_COMPLETE

	// ErrCodeKMSThrottlingException is defined in the API Reference https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.GetRecords
	// But it's not a constant?
	ErrCodeKMSThrottlingException = "KMSThrottlingException"
)

// ExtendedSequenceNumber represents a two-part sequence number for record aggregated by the Kinesis Producer Library.
//
// The KPL combines multiple user record into a single Kinesis record. Each user record therefore has an integer
// sub-sequence number, in addition to the regular sequence number of the Kinesis record. The sub-sequence number
// is used to checkpoint within an aggregated record.
type ExtendedSequenceNumber struct {
	SequenceNumber    *string
	SubSequenceNumber int64
}

type InitializationInput struct {
	ShardId                         string
	ExtendedSequenceNumber          *ExtendedSequenceNumber
	PendingCheckpointSequenceNumber *ExtendedSequenceNumber
}

type Status struct {
	ID            string
	ParentShardId string
	Checkpoint    string
	AssignedTo    string
	Mux           *sync.Mutex
	LeaseTimeout  time.Time
	// Shard Range
	StartingSequenceNumber string
	// child shard doesn't have end sequence number
	EndingSequenceNumber string
}

func (ss *Status) GetLeaseOwner() string {
	ss.Mux.Lock()
	defer ss.Mux.Unlock()
	return ss.AssignedTo
}

func (ss *Status) SetLeaseOwner(owner string) {
	ss.Mux.Lock()
	defer ss.Mux.Unlock()
	ss.AssignedTo = owner
}

type ConsumerState int

// ShardConsumer is responsible for consuming data record of a (specified) shard.
// Note: ShardConsumer only deal with one shard.
type Consumer struct {
	streamName      string
	shard           *Status
	kc              kinesisiface.KinesisAPI
	checkpointer    Checkpointer
	recordProcessor record.IRecordProcessor
	kclConfig       *goKCL.KinesisClientLibConfiguration
	stop            *chan struct{}
	waitGroup       *sync.WaitGroup
	consumerID      string
	mService        util.MonitoringService
	state           ConsumerState
}

func (sc *Consumer) getShardIterator(st *Status) (*string, error) {
	// Get checkpoint of the shard from dynamoDB
	err := sc.checkpointer.FetchCheckpoint(st)
	if err != nil && err != ErrSequenceIDNotFound {
		return nil, err
	}

	// If there isn't any checkpoint for the shard, use the configuration value.
	if st.Checkpoint == "" {
		initPos := sc.kclConfig.InitialPositionInStream
		log.Debugf("No checkpoint recorded for shard: %v, starting with: %v", st.ID,
			aws.StringValue(goKCL.InitalPositionInStreamToShardIteratorType(initPos)))
		shardIterArgs := &kinesis.GetShardIteratorInput{
			ShardId:           &st.ID,
			ShardIteratorType: goKCL.InitalPositionInStreamToShardIteratorType(initPos),
			StreamName:        &sc.streamName,
		}
		iterResp, err := sc.kc.GetShardIterator(shardIterArgs)
		if err != nil {
			return nil, err
		}
		return iterResp.ShardIterator, nil
	}

	log.Debugf("Start shard: %v at checkpoint: %v", st.ID, st.Checkpoint)
	shardIterArgs := &kinesis.GetShardIteratorInput{
		ShardId:                &st.ID,
		ShardIteratorType:      aws.String("AFTER_SEQUENCE_NUMBER"),
		StartingSequenceNumber: &st.Checkpoint,
		StreamName:             &sc.streamName,
	}
	iterResp, err := sc.kc.GetShardIterator(shardIterArgs)
	if err != nil {
		return nil, err
	}
	return iterResp.ShardIterator, nil
}

// getRecords continously poll one shard for data record
// Precondition: it currently has the lease on the shard.
func (sc *Consumer) GetRecords(shard *Status) error {
	defer sc.waitGroup.Done()
	defer sc.releaseLease(shard)

	// If the shard is child shard, need to wait until the parent finished.
	if err := sc.waitOnParentShard(shard); err != nil {
		// If parent shard has been deleted by Kinesis system already, just ignore the error.
		if err != ErrSequenceIDNotFound {
			log.Errorf("Error in waiting for parent shard: %v to finish. Error: %+v", shard.ParentShardId, err)
			return err
		}
	}

	shardIterator, err := sc.getShardIterator(shard)
	if err != nil {
		log.Errorf("Unable to get shard iterator for %s: %v", shard.ID, err)
		return err
	}

	// Start processing events and notify record processor on shard and starting checkpoint
	input := &InitializationInput{
		ShardId:                shard.ID,
		ExtendedSequenceNumber: &ExtendedSequenceNumber{SequenceNumber: aws.String(shard.Checkpoint)},
	}
	sc.recordProcessor.Initialize(input)

	recordCheckpointer := record.NewRecordProcessorCheckpoint(shard, sc.checkpointer)
	retriedErrors := 0

	for {
		getRecordsStartTime := time.Now()
		if time.Now().UTC().After(shard.LeaseTimeout.Add(-5 * time.Second)) {
			log.Debugf("Refreshing lease on shard: %s for worker: %s", shard.ID, sc.consumerID)
			err = sc.checkpointer.GetLease(shard, sc.consumerID)
			if err != nil {
				if err.Error() == ErrLeaseNotAquired {
					log.Warnf("Failed in acquiring lease on shard: %s for worker: %s", shard.ID, sc.consumerID)
					return nil
				}
				// log and return error
				log.Errorf("Error in refreshing lease on shard: %s for worker: %s. Error: %+v",
					shard.ID, sc.consumerID, err)
				return err
			}
		}

		log.Debugf("Trying to read %d record from iterator: %v", sc.kclConfig.MaxRecords, aws.StringValue(shardIterator))
		getRecordsArgs := &kinesis.GetRecordsInput{
			Limit:         aws.Int64(int64(sc.kclConfig.MaxRecords)),
			ShardIterator: shardIterator,
		}
		// Get record from stream and retry as needed
		getResp, err := sc.kc.GetRecords(getRecordsArgs)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException || awsErr.Code() == ErrCodeKMSThrottlingException {
					log.Errorf("Error getting record from shard %v: %+v", shard.ID, err)
					retriedErrors++
					// exponential backoff
					// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff
					time.Sleep(time.Duration(math.Exp2(float64(retriedErrors))*100) * time.Millisecond)
					continue
				}
			}
			log.Errorf("Error getting record from Kinesis that cannot be retried: %+v Request: %s", err, getRecordsArgs)
			return err
		}

		// reset the retry count after success
		retriedErrors = 0

		// IRecordProcessorCheckpointer
		input := &record.ProcessRecordsInput{
			Records:            getResp.Records,
			MillisBehindLatest: aws.Int64Value(getResp.MillisBehindLatest),
			Checkpointer:       recordCheckpointer,
		}

		recordLength := len(input.Records)
		recordBytes := int64(0)
		log.Debugf("Received %d record, MillisBehindLatest: %v", recordLength, input.MillisBehindLatest)

		for _, r := range getResp.Records {
			recordBytes += int64(len(r.Data))
		}

		if recordLength > 0 || sc.kclConfig.CallProcessRecordsEvenForEmptyRecordList {
			processRecordsStartTime := time.Now()

			// Delivery the events to the record processor
			sc.recordProcessor.ProcessRecords(input)

			// Convert from nanoseconds to milliseconds
			processedRecordsTiming := time.Since(processRecordsStartTime) / 1000000
			sc.mService.RecordProcessRecordsTime(shard.ID, float64(processedRecordsTiming))
		}

		sc.mService.IncrRecordsProcessed(shard.ID, recordLength)
		sc.mService.IncrBytesProcessed(shard.ID, recordBytes)
		sc.mService.MillisBehindLatest(shard.ID, float64(*getResp.MillisBehindLatest))

		// Convert from nanoseconds to milliseconds
		getRecordsTime := time.Since(getRecordsStartTime) / 1000000
		sc.mService.RecordGetRecordsTime(shard.ID, float64(getRecordsTime))

		// Idle between each read, the user is responsible for checkpoint the progress
		// This value is only used when no record are returned; if record are returned, it should immediately
		// retrieve the next set of record.
		if recordLength == 0 && aws.Int64Value(getResp.MillisBehindLatest) < int64(sc.kclConfig.IdleTimeBetweenReadsInMillis) {
			time.Sleep(time.Duration(sc.kclConfig.IdleTimeBetweenReadsInMillis) * time.Millisecond)
		}

		// The shard has been closed, so no new record can be read from it
		if getResp.NextShardIterator == nil {
			log.Infof("Shard %s closed", shard.ID)
			shutdownInput := &util.ShutdownInput{ShutdownReason: util.TERMINATE, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		}
		shardIterator = getResp.NextShardIterator

		select {
		case <-*sc.stop:
			shutdownInput := &util.ShutdownInput{ShutdownReason: util.REQUESTED, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		case <-time.After(1 * time.Nanosecond):
		}
	}
}

// Need to wait until the parent shard finished
func (sc *Consumer) waitOnParentShard(shard *Status) error {
	if len(shard.ParentShardId) == 0 {
		return nil
	}

	pshard := &Status{
		ID:  shard.ParentShardId,
		Mux: &sync.Mutex{},
	}

	for {
		if err := sc.checkpointer.FetchCheckpoint(pshard); err != nil {
			return err
		}

		// Parent shard is finished.
		if pshard.Checkpoint == SHARD_END {
			return nil
		}

		time.Sleep(time.Duration(sc.kclConfig.ParentShardPollIntervalMillis) * time.Millisecond)
	}
}

// Cleanup the internal lease cache
func (sc *Consumer) releaseLease(shard *Status) {
	log.Infof("Release lease for shard %s", shard.ID)
	shard.SetLeaseOwner("")

	// Release the lease by wiping out the lease owner for the shard
	// Note: we don't need to do anything in case of error here and shard lease will eventuall be expired.
	if err := sc.checkpointer.RemoveLeaseOwner(shard.ID); err != nil {
		log.Errorf("Failed to release shard lease or shard: %s Error: %+v", shard.ID, err)
	}

	// reporting lease lose metrics
	sc.mService.LeaseLost(shard.ID)
}
