package goKCL

import (
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/guygma/goKCL/record"
	"github.com/guygma/goKCL/shard"
	"github.com/guygma/goKCL/util"
)

//TODO: Determine if any of these fields need to be exported (capitalized).
// High level struct that governs KCL execution.
type Worker struct {
	streamName string
	regionName string
	workerID   string

	processorFactory record.IRecordProcessorFactory
	kclConfig        *KinesisClientLibConfiguration
	kc               kinesisiface.KinesisAPI
	checkpointer     shard.Checkpointer

	stop      *chan struct{}
	waitGroup *sync.WaitGroup
	done      bool

	shardStatus map[string]*shard.Status

	metricsConfig *util.MonitoringConfiguration
	mService      util.MonitoringService
}

// NewWorker constructs a Worker instance for processing Kinesis stream data.
//TODO: This should be a method on the Worker type named "New".
func NewWorker(factory record.IRecordProcessorFactory,
	kclConfig *KinesisClientLibConfiguration,
	metricsConfig *util.MonitoringConfiguration) *Worker {
	w := &Worker{
		streamName:       kclConfig.StreamName,
		regionName:       kclConfig.RegionName,
		workerID:         kclConfig.WorkerID,
		processorFactory: factory,
		kclConfig:        kclConfig,
		metricsConfig:    metricsConfig,
		done:             false,
	}

	if w.metricsConfig == nil {
		// "" means noop monitor service. i.e. not emitting any metrics.
		w.metricsConfig = &util.MonitoringConfiguration{MonitoringService: ""}
	}
	return w
}

// WithKinesis is used to provide Kinesis service for either custom implementation or unit testing.
func (w *Worker) WithKinesis(svc kinesisiface.KinesisAPI) *Worker {
	w.kc = svc
	return w
}

// WithCheckpointer is used to provide a custom checkpointer service for non-dynamodb implementation
// or unit testing.
func (w *Worker) WithCheckpointer(checker shard.Checkpointer) *Worker {
	w.checkpointer = checker
	return w
}

// Run starts consuming data from the stream, and pass it to the application record processors.
func (w *Worker) Start() error {
	if err := w.initialize(); err != nil {
		log.Errorf("Failed to initialize Worker: %+v", err)
		return err
	}

	// Start monitoring service
	log.Info("Starting monitoring service.")
	if err := w.mService.Start(); err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
		return err
	}

	log.Info("Starting worker event loop.")
	// entering event loop
	go w.eventLoop()
	return nil
}

// Shutdown signals worker to shutdown. Worker will try initiating shutdown of all record processors.
func (w *Worker) Shutdown() {
	log.Info("Worker shutdown is requested.")

	if w.done {
		return
	}

	close(*w.stop)
	w.done = true
	w.waitGroup.Wait()

	w.mService.Shutdown()
	log.Info("Worker loop is complete. Exiting from worker.")
}

// Publish to write some data into stream. This function is mainly used for testing purpose.
func (w *Worker) Publish(streamName, partitionKey string, data []byte) error {
	_, err := w.kc.PutRecord(&kinesis.PutRecordInput{
		Data:         data,
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(partitionKey),
	})
	if err != nil {
		log.Errorf("Error in publishing data to %s/%s. Error: %+v", streamName, partitionKey, err)
	}
	return err
}

// initialize
func (w *Worker) initialize() error {
	log.Info("Worker initialization in progress...")

	// Create default Kinesis session
	if w.kc == nil {
		// create session for Kinesis
		log.Info("Creating Kinesis session")

		s, err := session.NewSession(&aws.Config{
			Region:      aws.String(w.regionName),
			Endpoint:    &w.kclConfig.KinesisEndpoint,
			Credentials: w.kclConfig.KinesisCredentials,
		})

		if err != nil {
			// no need to move forward
			log.Fatalf("Failed in getting Kinesis session for creating Worker: %+v", err)
		}
		w.kc = kinesis.New(s)
	} else {
		log.Info("Use custom Kinesis service.")
	}

	// Create default dynamodb based checkpointer implementation
	if w.checkpointer == nil {
		log.Info("Creating DynamoDB based checkpointer")
		w.checkpointer = shard.NewDynamoCheckpoint(w.kclConfig)
	} else {
		log.Info("Use custom checkpointer implementation.")
	}

	err := w.metricsConfig.Init(w.kclConfig.ApplicationName, w.streamName, w.workerID)
	if err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
	}
	w.mService = w.metricsConfig.GetMonitoringService()

	log.Info("Initializing Checkpointer")
	if err := w.checkpointer.Init(); err != nil {
		log.Errorf("Failed to start Checkpointer: %+v", err)
		return err
	}

	w.shardStatus = make(map[string]*shard.Status)

	stopChan := make(chan struct{})
	w.stop = &stopChan

	wg := sync.WaitGroup{}
	w.waitGroup = &wg

	log.Info("Initialization complete.")

	return nil
}

// newShardConsumer to create a shard consumer instance
func (w *Worker) newShardConsumer(shard *shard.Status) *shard.Consumer {
	s := &shard.Consumer{
		streamName:      w.streamName,
		shard:           shard,
		kc:              w.kc,
		checkpointer:    w.checkpointer,
		recordProcessor: w.processorFactory.CreateProcessor(),
		kclConfig:       w.kclConfig,
		consumerID:      w.workerID,
		stop:            w.stop,
		waitGroup:       w.waitGroup,
		mService:        w.mService,
		state:           shard.WAITING_ON_PARENT_SHARDS,
	}
	return s
}

// eventLoop
func (w *Worker) eventLoop() {
	for {
		err := w.syncShard()
		if err != nil {
			log.Errorf("Error getting Kinesis shards: %+v", err)
			time.Sleep(time.Duration(w.kclConfig.ShardSyncIntervalMillis) * time.Millisecond)
			continue
		}

		log.Infof("Found %d shards", len(w.shardStatus))

		// Count the number of leases hold by this worker excluding the processed sh
		counter := 0
		for _, sh := range w.shardStatus {
			if sh.GetLeaseOwner() == w.workerID && sh.Checkpoint != shard.SHARD_END {
				counter++
			}
		}

		// max number of lease has not been reached yet
		if counter < w.kclConfig.MaxLeasesForWorker {
			for _, sh := range w.shardStatus {
				// already owner of the sh
				if sh.GetLeaseOwner() == w.workerID {
					continue
				}

				err := w.checkpointer.FetchCheckpoint(sh)
				if err != nil {
					// checkpoint may not existed yet is not an error condition.
					if err != shard.ErrSequenceIDNotFound {
						log.Errorf(" Error: %+v", err)
						// move on to next sh
						continue
					}
				}

				// The sh is closed and we have processed all record
				if sh.Checkpoint == shard.SHARD_END {
					continue
				}

				err = w.checkpointer.GetLease(sh, w.workerID)
				if err != nil {
					// cannot get lease on the sh
					if err.Error() != shard.ErrLeaseNotAquired {
						log.Error(err)
					}
					continue
				}

				// log metrics on got lease
				w.mService.LeaseGained(sh.ID)

				log.Infof("Start Shard Consumer for sh: %v", sh.ID)
				sc := w.newShardConsumer(sh)
				go sc.GetRecords(sh) // Need to handle the error using a channel or rework the goroutine
				w.waitGroup.Add(1)
				// exit from for loop and do not get any more shards for now.
				break
			}
		}

		select {
		case <-*w.stop:
			log.Info("Shutting down...")
			return
		case <-time.After(time.Duration(w.kclConfig.ShardSyncIntervalMillis) * time.Millisecond):
		}
	}
}

// List all ACTIVE shard and store them into shardStatus table
// If shard has been removed, need to exclude it from cached shard status.
func (w *Worker) getShardIDs(startShardID string, shardInfo map[string]bool) error {
	// The default pagination limit is 100.
	args := &kinesis.DescribeStreamInput{
		StreamName: aws.String(w.streamName),
	}

	if startShardID != "" {
		args.ExclusiveStartShardId = aws.String(startShardID)
	}

	streamDesc, err := w.kc.DescribeStream(args)
	if err != nil {
		log.Errorf("Error in DescribeStream: %s Error: %+v Request: %s", w.streamName, err, args)
		return err
	}

	if *streamDesc.StreamDescription.StreamStatus != "ACTIVE" {
		log.Warnf("Stream %s is not active", w.streamName)
		return errors.New("stream not active")
	}

	var lastShardID string
	for _, s := range streamDesc.StreamDescription.Shards {
		// record avail shardId from fresh reading from Kinesis
		shardInfo[*s.ShardId] = true

		// found new shard
		if _, ok := w.shardStatus[*s.ShardId]; !ok {
			log.Infof("Found new shard with id %s", *s.ShardId)
			w.shardStatus[*s.ShardId] = &shard.Status{
				ID:                     *s.ShardId,
				ParentShardId:          aws.StringValue(s.ParentShardId),
				Mux:                    &sync.Mutex{},
				StartingSequenceNumber: aws.StringValue(s.SequenceNumberRange.StartingSequenceNumber),
				EndingSequenceNumber:   aws.StringValue(s.SequenceNumberRange.EndingSequenceNumber),
			}
		}
		lastShardID = *s.ShardId
	}

	if *streamDesc.StreamDescription.HasMoreShards {
		err := w.getShardIDs(lastShardID, shardInfo)
		if err != nil {
			log.Errorf("Error in getShardIDs: %s Error: %+v", lastShardID, err)
			return err
		}
	}

	return nil
}

// syncShard to sync the cached shard info with actual shard info from Kinesis
func (w *Worker) syncShard() error {
	shardInfo := make(map[string]bool)
	err := w.getShardIDs("", shardInfo)

	if err != nil {
		return err
	}

	for _, sh := range w.shardStatus {
		// The cached shard no longer existed, remove it.
		if _, ok := shardInfo[sh.ID]; !ok {
			// remove the shard from local status cache
			delete(w.shardStatus, sh.ID)
			// remove the shard entry in dynamoDB as well
			// Note: syncShard runs periodically. we don't need to do anything in case of error here.
			if err := w.checkpointer.RemoveLeaseInfo(sh.ID); err != nil {
				log.Errorf("Failed to remove shard lease info: %s Error: %+v", sh.ID, err)
			}
		}
	}

	return nil
}
