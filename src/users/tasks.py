from celery import shared_task
from django.utils import timezone
from datetime import timedelta
from django.db import transaction
from users.models import Outbox
from core.event_log_client import EventLogClient
import structlog
from sentry_sdk import capture_message, start_transaction

# Logger for structured logging
logger = structlog.get_logger(__name__)

@shared_task(
    autoretry_for=(Exception,),  # Automatically retry the task in case of failure
    retry_backoff=60,  # Retry delay, increases with each retry
    max_retries=3,  # Maximum number of retries before giving up
    soft_time_limit=300,  # Soft time limit in seconds, after which the task should be considered slow
    time_limit=330  # Hard time limit in seconds, after which the task is terminated
)
def process_outbox() -> None:
    """
    Processes the outbox queue, updating the status of the messages and
    inserting them into ClickHouse. Handles pending and failed messages.
    The task retries on failure and logs all operations.

    Steps performed:
    1. Marks stale messages (processing for more than 10 minutes) as failed.
    2. Selects up to 500 pending or failed logs for processing.
    3. Updates the status of the selected logs to 'processing'.
    4. Inserts the logs into ClickHouse via EventLogClient.
    5. Marks successfully processed logs as 'processed'.
    6. Handles exceptions and logs errors, marking failed logs as 'failed'.
    """
    with start_transaction(op="task", name="process_outbox"):  # Start a transaction in Sentry for tracing
        pending_logs_ids = []  # List to store the IDs of logs being processed

        try:
            # 1. Handling stale records
            stale_threshold = timezone.now() - timedelta(minutes=10)
            with transaction.atomic():  # Ensure atomicity of database updates
                # Mark records that are still processing for more than 10 minutes as failed
                Outbox.objects.filter(
                    status=Outbox.STATUS_PROCESSING,
                    updated_at__lte=stale_threshold
                ).update(status=Outbox.STATUS_FAILED)

            # 2. Select records for processing (pending or failed)
            pending_logs = list(Outbox.objects.filter(
                status__in=[Outbox.STATUS_PENDING, Outbox.STATUS_FAILED]
            ).order_by('created_at')[:500])  # Limit to 500 records to process at once

            if not pending_logs:  # If no logs to process, exit early
                logger.info("No pending logs to process")
                return

            # Collect IDs of the logs to be updated
            pending_logs_ids = [log.id for log in pending_logs]

            # 3. Update the status to 'processing'
            with transaction.atomic():
                Outbox.objects.filter(id__in=pending_logs_ids).update(
                    status=Outbox.STATUS_PROCESSING,
                    updated_at=timezone.now()  # Update the timestamp of when the processing started
                )

            # 4. Insert logs into ClickHouse (external service for storing logs)
            with EventLogClient.init() as client:
                client.insert(pending_logs)

            # 5. Mark logs as 'processed' upon successful completion
            with transaction.atomic():
                Outbox.objects.filter(id__in=pending_logs_ids).update(
                    status=Outbox.STATUS_PROCESSED,
                    updated_at=timezone.now()  # Update the timestamp to reflect successful processing
                )
                logger.info("Processed batch", size=len(pending_logs))  # Log the batch size

        except Exception as e:
            # 6. Handle exceptions by logging error and updating failed logs
            logger.error("Processing failed", error=str(e))
            if pending_logs_ids:  # If there were logs being processed, mark them as 'failed'
                with transaction.atomic():
                    Outbox.objects.filter(id__in=pending_logs_ids).update(
                        status=Outbox.STATUS_FAILED,
                        updated_at=timezone.now()  # Update timestamp for failed status
                    )
            # Send the error message to Sentry for tracking
            capture_message(f"Outbox processing error: {str(e)}")
            raise  # Reraise the exception after logging and sending to Sentry
