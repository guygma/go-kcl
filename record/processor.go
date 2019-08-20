package record

import (
	"time"
)

type ProcessRecordsInput struct {
	CacheEntryTime     *time.Time
	CacheExitTime      *time.Time
	Records            []*ks.Record
	Checkpointer       record.IRecordProcessorCheckpointer
	MillisBehindLatest int64
}

type (
	// IRecordProcessor is the interface for some callback functions invoked by KCL will
	// The main task of using KCL is to provide implementation on IRecordProcessor interface.
	// Note: This is exactly the same interface as Amazon KCL IRecordProcessor v2
	IRecordProcessor interface {
		/**
		 * Invoked by the Amazon Kinesis Client Library before data record are delivered to the RecordProcessor instance
		 * (via processRecords).
		 *
		 * @param initializationInput Provides information related to initialization
		 */
		Initialize(initializationInput *checkpoint.InitializationInput)

		/**
		 * Process data record. The Amazon Kinesis Client Library will invoke this method to deliver data record to the
		 * application.
		 * Upon fail over, the new instance will get record with sequence number > checkpoint position
		 * for each partition key.
		 *
		 * @param processRecordsInput Provides the record to be processed as well as information and capabilities related
		 *        to them (eg checkpointing).
		 */
		ProcessRecords(processRecordsInput *checkpoint.ProcessRecordsInput)

		/**
		 * Invoked by the Amazon Kinesis Client Library to indicate it will no longer send data record to this
		 * RecordProcessor instance.
		 *
		 * <h2><b>Warning</b></h2>
		 *
		 * When the value of {@link ShutdownInput#getShutdownReason()} is
		 * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason#TERMINATE} it is required that you
		 * checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
		 *
		 * @param shutdownInput
		 *            Provides information and capabilities (eg checkpointing) related to shutdown of this record processor.
		 */
		Shutdown(shutdownInput *checkpoint.ShutdownInput)
	}

	// IRecordProcessorFactory is interface for creating IRecordProcessor. Each Worker can have multiple threads
	// for processing shard. Client can choose either creating one processor per shard or sharing them.
	IRecordProcessorFactory interface {

		/**
		 * Returns a record processor to be used for processing data record for a (assigned) shard.
		 *
		 * @return Returns a processor object.
		 */
		CreateProcessor() IRecordProcessor
	}
)

type (
	IPreparedCheckpointer interface {
		GetPendingCheckpoint() *ExtendedSequenceNumber

		/**
		 * This method will record a pending checkpoint.
		 *
		 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these record already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
		 *         backoff and retry.
		 * @error IllegalArgumentError The sequence number being checkpointed is invalid because it is out of range,
		 *         i.e. it is smaller than the last check point value (prepared or committed), or larger than the greatest
		 *         sequence number seen by the associated record processor.
		 */
		Checkpoint() error
	}

	/**
	 * Used by RecordProcessors when they want to checkpoint their progress.
	 * The Kinesis Client Library will pass an object implementing this interface to RecordProcessors, so they can
	 * checkpoint their progress.
	 */
	IRecordProcessorCheckpointer interface {
		/**
		 * This method will checkpoint the progress at the provided sequenceNumber. This method is analogous to
		 * {@link #checkpoint()} but provides the ability to specify the sequence number at which to
		 * checkpoint.
		 *
		 * @param sequenceNumber A sequence number at which to checkpoint in this shard. Upon failover,
		 *        the Kinesis Client Library will start fetching record after this sequence number.
		 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these record already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
		 *         backoff and retry.
		 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
		 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
		 *         greatest sequence number seen by the associated record processor.
		 *         2.) It is not a valid sequence number for a record in this shard.
		 */
		Checkpoint(sequenceNumber *string) error

		/**
		 * This method will record a pending checkpoint at the provided sequenceNumber.
		 *
		 * @param sequenceNumber A sequence number at which to prepare checkpoint in this shard.

		 * @return an IPreparedCheckpointer object that can be called later to persist the checkpoint.
		 *
		 * @error ThrottlingError Can't store pending checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these record already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store pending checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the pending checkpoint. The
		 *         application can backoff and retry.
		 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
		 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
		 *         greatest sequence number seen by the associated record processor.
		 *         2.) It is not a valid sequence number for a record in this shard.
		 */
		PrepareCheckpoint(sequenceNumber *string) (IPreparedCheckpointer, error)
	}
)
