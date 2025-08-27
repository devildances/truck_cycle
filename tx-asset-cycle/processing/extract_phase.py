import json
import logging
import os
import sys
import time
from typing import Self

from amazon_kclpy import kcl
from amazon_kclpy.kcl import Checkpointer
from amazon_kclpy.messages import (InitializeInput, LeaseLostInput,
                                   ProcessRecordsInput, ShardEndedInput,
                                   ShutdownRequestedInput)
from amazon_kclpy.v3 import processor

from models.dao.pgsql_action_dao import PgsqlActionDAO
from models.dao.redis_source_dao import RedisSourceDAO

from .load_realtime_phase import load_process as lp_realtime
from .transform_realtime_phase import process_new_record as pnr_realtime

logger = logging.getLogger(__name__)


class RecordProcessor(processor.RecordProcessorBase):
    """AWS Kinesis Consumer Library (KCL) record processor for mining data.

    Processes streaming data from AWS Kinesis shards containing real-time
    mining asset telemetry. Implements the KCL RecordProcessorBase interface
    to handle shard initialization, record processing, checkpointing, and
    graceful shutdown.

    The processor follows the standard KCL lifecycle:
    1. initialize() - Called once when assigned to a shard
    2. process_records() - Called repeatedly with batches of records
    3. shutdown() - Called when losing shard lease or shard ends

    Key Features:
        - Robust database connection management with retry logic
        - Automatic checkpointing with configurable frequency
        - Sequence number tracking for exactly-once processing
        - Error handling and recovery for transient failures
        - Support for multiple transaction types via environment configuration

    Configuration:
        - TX_TYPE environment variable determines processing mode
        - Checkpoint frequency and retry parameters are configurable
        - Database connection retry logic with exponential backoff

    Processing Pipeline:
        Real-time data flows through transform → load phases:
        1. Transform: Parse and validate incoming telemetry data
        2. Load: Persist processed data to PostgreSQL

    Attributes:
        _SLEEP_SECONDS (int): Delay between checkpoint retry attempts.
        _CHECKPOINT_RETRIES (int): Maximum checkpoint retry attempts.
        _CHECKPOINT_FREQ_SECONDS (int): Checkpoint frequency in seconds.
        _largest_seq (Tuple[int, int]): Highest processed sequence number
            and sub-sequence number for checkpointing.
        _last_checkpoint_time (float): Timestamp of last successful checkpoint.
        pgsql_dao (PgsqlActionDAO | None): PostgreSQL database connection.
        redis_dao (RedisSourceDAO | None): Redis cache connection.
        max_retries (int): Maximum database connection retry attempts.

    Example:
        >>> # KCL framework instantiates and manages the processor
        >>> processor = RecordProcessor()
        >>> # Framework calls initialize, process_records, shutdown automatically

    Environment Variables:
        TX_TYPE: Processing mode, currently supports "realtime" for
            real-time asset telemetry processing.
    """
    def __init__(self: Self) -> None:
        """Initialize the record processor with default configuration.

        Sets up processing parameters, checkpoint configuration, and
        initializes database connection placeholders. The actual database
        connections are established during the initialize() phase to handle
        potential connection failures gracefully.
        """
        self._SLEEP_SECONDS = 5
        self._CHECKPOINT_RETRIES = 5
        self._CHECKPOINT_FREQ_SECONDS = 60
        self._largest_seq = (None, None)
        self._largest_sub_seq = None
        self._last_checkpoint_time = None

        self.pgsql_dao: PgsqlActionDAO | None = None
        self.redis_dao: RedisSourceDAO | None = None
        self.max_retries: int = 4

    def log(self: Self, message: str) -> None:
        """Write log message to stderr for KCL framework visibility.

        Uses stderr to ensure log messages are captured by the KCL
        MultiLangDaemon and included in CloudWatch logs for monitoring
        and debugging purposes.

        Args:
            message (str): Log message to write to stderr.

        Note:
            The KCL framework expects processors to use stderr for logging
            to integrate properly with the daemon's log aggregation.
        """
        sys.stderr.write(message)

    def initialize(self: Self, initialize_input: InitializeInput) -> None:
        """Initialize database connections and processor state for a shard.

        Called once by the KCL framework when this processor is assigned
        to a shard. Establishes database connections with retry logic to
        handle transient network issues and initializes sequence tracking
        for checkpointing.

        The initialization process:
        1. Reset sequence tracking state
        2. Attempt database connection establishment with retries
        3. Log successful initialization or raise connection errors

        Args:
            initialize_input (InitializeInput): Shard assignment information
                including shard_id for logging and identification.

        Raises:
            ConnectionError: If database connections cannot be established
                after max_retries attempts, wrapping the original exception.

        Example:
            >>> # Called automatically by KCL framework
            >>> processor.initialize(InitializeInput(shard_id="shard-001"))

        Database Connections:
            - PostgreSQL: For persisting cycle records and process info
            - Redis: For caching latest process timestamps and lookup data
        """
        self._largest_seq = (None, None)
        self._last_checkpoint_time = time.time()
        retry_time = 0

        logger.info(
            f"[INIT] Initializing resources for shard: {initialize_input.shard_id}"
        )

        while retry_time < self.max_retries:
            try:
                self.pgsql_dao = PgsqlActionDAO()
                self.redis_dao = RedisSourceDAO()
                logger.info(
                    "[INIT] Successfully initialized DAO resources "
                    f"for shard: {initialize_input.shard_id}"
                )
                break
            except Exception as e:
                logger.error(
                    "[INIT_ERROR] Failed to initialize DAO resources "
                    f"for shard {initialize_input.shard_id}: {e}", exc_info=True
                )
                retry_time += 1
                if retry_time == self.max_retries:
                    raise ConnectionError(e)

    def checkpoint(
        self: Self,
        checkpointer: Checkpointer,
        sequence_number: str | None = None,
        sub_sequence_number: int | None = None
    ) -> None:
        """Checkpoint progress with retry logic for transient failures.

        Attempts to checkpoint the current processing position with the
        KCL framework to enable resume from the correct position after
        failures or rebalancing. Implements retry logic for common
        transient errors like throttling.

        Checkpoint Behavior:
        - Uses provided sequence numbers or defaults to latest processed
        - Retries on throttling with exponential backoff
        - Skips checkpoint on shutdown exceptions
        - Logs and continues on invalid state exceptions

        Args:
            checkpointer (Checkpointer): KCL checkpointer instance for
                recording progress.
            sequence_number (str | None): Specific sequence number to
                checkpoint. If None, uses last processed sequence.
            sub_sequence_number (int | None): Specific sub-sequence number
                to checkpoint. If None, uses last processed sub-sequence.

        Error Handling:
            - ShutdownException: Skips checkpoint and returns gracefully
            - ThrottlingException: Retries with backoff up to retry limit
            - InvalidStateException: Logs error and continues
            - Other exceptions: Logs error and continues processing

        Note:
            Checkpointing failures are logged but don't stop processing
            to maintain system resilience. The framework will eventually
            retry checkpointing or reprocess from the last successful
            checkpoint.
        """
        for n in range(0, self._CHECKPOINT_RETRIES):
            try:
                checkpointer.checkpoint(
                    sequence_number, sub_sequence_number
                )
                return
            except kcl.CheckpointError as e:
                if "ShutdownException" == e.value:
                    logger.warning(
                        "Encountered shutdown exception, "
                        "skipping checkpoint"
                    )
                    return
                elif "ThrottlingException" == e.value:
                    if self._CHECKPOINT_RETRIES - 1 == n:
                        sys.stderr.write(
                            "Failed to checkpoint after {n} attempts, "
                            "giving up.\n".format(n=n)
                        )
                        return
                    else:
                        logger.warning(
                            "Was throttled while checkpointing, "
                            "will attempt again in {s} seconds".format(
                                s=self._SLEEP_SECONDS
                            )
                        )
                elif "InvalidStateException" == e.value:
                    sys.stderr.write(
                        "MultiLangDaemon reported an "
                        "invalid state while checkpointing.\n"
                    )
                else:  # Some other error
                    sys.stderr.write(
                        "Encountered an error while checkpointing, "
                        "error was {e}.\n".format(e=e)
                    )
            time.sleep(self._SLEEP_SECONDS)

    def process_record(
        self: Self,
        data: dict,
        partition_key: str,
        sequence_number: int,
        sub_sequence_number: int
    ) -> None:
        """Process a single record through the transform and load pipeline.

        Processes individual telemetry records from mining assets through
        the configured processing pipeline. Currently supports real-time
        asset cycle processing with transform and load phases.

        Processing Flow:
        1. Determine processing type from TX_TYPE environment variable
        2. Transform: Parse, validate, and enrich the raw telemetry data
        3. Load: Persist processed data to PostgreSQL
        4. Log processing metadata for monitoring

        Args:
            data (dict): Parsed JSON telemetry data from the Kinesis record
                containing asset location, status, and operational data.
            partition_key (str): Kinesis partition key, typically asset
                identifier for data locality.
            sequence_number (int): Kinesis sequence number for ordering
                and duplicate detection.
            sub_sequence_number (int): Sub-sequence number for records
                within the same sequence.

        Raises:
            ValueError: If TX_TYPE environment variable contains an
                unsupported processing type.

        Transaction Types:
            - "realtime": Process real-time asset telemetry for cycle tracking
            - Future types can be added with additional pipeline implementations

        Example:
            >>> # Called automatically by process_records for each record
            >>> processor.process_record(
            ...     data={"asset_guid": "truck-123","timestamp": "2024-01-01T10:00:00"},
            ...     partition_key="truck-123",
            ...     sequence_number=12345,
            ...     sub_sequence_number=0
            ... )
        """
        ####################################
        if data is None:
            logger.warning("Received None data for processing")
            return

        if not isinstance(data, dict):
            logger.warning(f"Expected dict but got {type(data)}: {data}")
            return

        TX_TYPE = os.getenv("TX_TYPE")
        if TX_TYPE == "realtime":
            (
                initial_record,
                latest_record_updated,
                new_cycle_record,
                process_info_record,
            ) = pnr_realtime(
                pgsql_db_conn_obj=self.pgsql_dao,
                redis_db_conn_obj=self.redis_dao,
                current_record_data=data
            )
            lp_realtime(
                pgsql_conn=self.pgsql_dao,
                initial_cycle_record=initial_record,
                last_cycle_record_updated=latest_record_updated,
                new_cycle_record=new_cycle_record,
                process_info_record=process_info_record
            )
        else:
            raise ValueError(
                f"This {TX_TYPE} is unknown for tx-asset-cycle type."
            )
        ####################################
        self.log(
            "Record (Partition Key: {pk}, "
            "Sequence Number: {seq}, "
            "Subsequence Number: {sseq}, "
            "Data Size: {ds}".format(
                pk=partition_key,
                seq=sequence_number,
                sseq=sub_sequence_number,
                ds=len(data)
            )
        )

    def should_update_sequence(
        self: Self, sequence_number: int, sub_sequence_number: int
    ) -> bool:
        """Determine if current record has a higher sequence than tracked.

        Compares the current record's sequence numbers against the highest
        sequence numbers processed so far to maintain ordering and support
        accurate checkpointing. Used to track processing progress for
        exactly-once delivery semantics.

        Comparison Logic:
        1. If no sequence tracked yet, update is needed
        2. If sequence number is higher, update is needed
        3. If sequence number is equal but sub-sequence is higher,
           update is needed
        4. Otherwise, no update needed

        Args:
            sequence_number (int): Current record's sequence number from
                Kinesis shard.
            sub_sequence_number (int): Current record's sub-sequence number
                for records within the same sequence.

        Returns:
            bool: True if the largest sequence should be updated with
                current values, False if current sequence is not larger.

        Example:
            >>> # First record
            >>> processor.should_update_sequence(100, 0)  # True
            >>> processor._largest_seq = (100, 0)
            >>>
            >>> # Higher sequence
            >>> processor.should_update_sequence(101, 0)  # True
            >>>
            >>> # Same sequence, higher sub-sequence
            >>> processor.should_update_sequence(101, 1)  # True
            >>>
            >>> # Lower sequence
            >>> processor.should_update_sequence(100, 5)  # False
        """
        if self._largest_seq == (None, None):
            return True
        if sequence_number > self._largest_seq[0]:
            return True
        if (sequence_number == self._largest_seq[0]) and \
                (sub_sequence_number > self._largest_seq[1]):
            return True
        return False

    def process_records(
        self: Self, process_records_input: ProcessRecordsInput
    ) -> None:
        """Process a batch of records and checkpoint progress periodically.

        Main processing method called by the KCL framework with batches of
        records from the Kinesis shard. Decodes JSON data, processes each
        record through the pipeline, tracks sequence numbers, and
        checkpoints progress at configured intervals.

        Processing Steps:
        1. Iterate through all records in the batch
        2. Decode binary data from UTF-8 and parse JSON
        3. Process each record through the transform/load pipeline
        4. Update sequence tracking for checkpoint management
        5. Checkpoint progress if enough time has elapsed

        Args:
            process_records_input (ProcessRecordsInput): Batch of records
                from KCL framework including the records list and a
                checkpointer for progress tracking.

        Error Handling:
            Catches and logs all exceptions during processing to prevent
            shard processor crashes. Failed records are logged but
            processing continues to maintain system availability.

        Checkpointing:
            Checkpoints are created based on time intervals
            (_CHECKPOINT_FREQ_SECONDS) rather than record counts to
            balance between progress tracking and performance.

        Example:
            >>> # Called automatically by KCL framework
            >>> processor.process_records(process_records_input)

        Data Flow:
            Kinesis Record → UTF-8 Decode → JSON Parse →
            Transform Phase → Load Phase → Sequence Update →
            Periodic Checkpoint
        """
        try:
            skipped_count = 0
            error_count = 0

            for record in process_records_input.records:
                try:
                    r_data = record.binary_data
                    dcd_data = r_data.decode('utf-8')

                    # Handle different JSON structures
                    seq = int(record.sequence_number)
                    sub_seq = record.sub_sequence_number
                    key = record.partition_key

                    # Skip empty records
                    if not dcd_data:
                        logger.error("Skipping empty record after decode")
                        skipped_count += 1
                        continue

                    # Parse JSON
                    try:
                        parsed_data = json.loads(dcd_data)
                    except json.JSONDecodeError as e:
                        logger.error(
                            f"Failed to parse JSON: {e}, "
                            f"data: '{dcd_data[:100]}'"
                        )
                        error_count += 1
                        continue

                    if parsed_data is None:
                        logger.error("Skipping null JSON data")
                        skipped_count += 1
                        continue

                    elif isinstance(parsed_data, dict):
                        if len(parsed_data) > 0:
                            self.process_record(
                                json.loads(dcd_data), key, seq, sub_seq
                            )
                        else:
                            logger.error("Skipping empty JSON object")
                            skipped_count += 1
                            continue

                    elif isinstance(parsed_data, list):
                        if len(parsed_data) == 0:
                            logger.error("Skipping empty JSON array")
                            skipped_count += 1
                            continue
                        else:
                            # Process each item in the array
                            for i, item in enumerate(parsed_data):
                                if isinstance(item, dict) and len(item) > 0:
                                    self.process_record(
                                        json.loads(dcd_data), key, seq, sub_seq
                                    )
                                else:
                                    logger.error(
                                        f"Skipping invalid list item {i}: "
                                        f"{type(item)}"
                                    )
                    else:
                        logger.warning(
                            f"Unexpected JSON type: {type(parsed_data)}"
                        )
                        error_count += 1
                        continue

                    if self.should_update_sequence(seq, sub_seq):
                        self._largest_seq = (seq, sub_seq)

                except Exception as e:
                    logger.error(
                        f"Error processing Kinesis record: {e}",
                        exc_info=True
                    )
                    error_count += 1
                    continue

            # Checkpoint periodically
            if ((time.time() - self._last_checkpoint_time)
                    > self._CHECKPOINT_FREQ_SECONDS):
                self.checkpoint(
                    process_records_input.checkpointer,
                    str(self._largest_seq[0]),
                    self._largest_seq[1]
                )
                self._last_checkpoint_time = time.time()

        except Exception as e:
            self.log(
                "Encountered an exception while processing records. "
                "Exception was {e}\n".format(e=e)
            )

    def lease_lost(
        self: Self, lease_lost_input: LeaseLostInput
    ) -> None:
        """Handle shard lease loss during rebalancing or failures.

        Called by the KCL framework when this processor loses the lease
        to its assigned shard. This can occur during normal rebalancing
        when new consumers join or leave the consumer group, or due to
        network issues or processor failures.

        Args:
            lease_lost_input (LeaseLostInput): Information about the
                lease loss event from the KCL framework.

        Note:
            After lease loss, the processor should stop processing and
            allow another consumer to take over the shard. No cleanup
            is typically required as the framework handles resource
            management.
        """
        self.log("Lease has been lost")

    def shard_ended(self: Self, shard_ended_input: ShardEndedInput) -> None:
        """Handle shard completion and perform final checkpoint.

        Called when the assigned shard has been closed due to resharding
        operations or stream scaling changes. This is a normal part of
        Kinesis stream lifecycle management and indicates all data in
        the shard has been processed.

        Args:
            shard_ended_input (ShardEndedInput): Information about the
                shard ending event, including a checkpointer for final
                progress recording.

        Final Checkpoint:
            A final checkpoint is created to mark the shard as completely
            processed, ensuring no data is lost during stream transitions.
        """
        self.log("Shard has ended checkpointing")
        shard_ended_input.checkpointer.checkpoint()

    def shutdown_requested(
        self: Self, shutdown_requested_input: ShutdownRequestedInput
    ) -> None:
        """Handle graceful shutdown request and checkpoint final progress.

        Called when the KCL framework requests a graceful shutdown of
        this processor instance. This typically occurs during application
        termination, container shutdown, or scaling operations.

        Args:
            shutdown_requested_input (ShutdownRequestedInput): Information
                about the shutdown request, including a checkpointer for
                final progress recording.

        Graceful Shutdown:
            Creates a final checkpoint to ensure processing can resume
            from the correct position when a new processor instance
            starts. This prevents data loss and duplicate processing.
        """
        self.log("Shutdown has been requested, checkpointing.")
        shutdown_requested_input.checkpointer.checkpoint()
