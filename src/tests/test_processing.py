import pytest
from django.utils import timezone
from users.models import Outbox
from users.tasks import process_outbox
from core.event_log_client import EventLogClient
import json

@pytest.fixture(autouse=True)
def setup_clickhouse():
    with EventLogClient.init() as client:
        client._client.command(
            "CREATE TABLE IF NOT EXISTS default.event_logs_test "
            "(event_type String, event_date_time DateTime, environment String, event_context String, metadata_version UInt64) "
            "ENGINE = MergeTree() ORDER BY (event_date_time)"
        )
    yield
    with EventLogClient.init() as client:
        client._client.command("TRUNCATE TABLE default.event_logs_test")

@pytest.mark.django_db
def test_outbox_processing():
    # Создание тестовых данных
    for i in range(3):
        Outbox.objects.create(
            event_type=f'TestEvent{i}',
            environment='test',
            event_context={'data': f'test{i}'},
            metadata_version=1
        )

    process_outbox()

    # Проверка статусов
    assert Outbox.objects.filter(status=Outbox.STATUS_PROCESSED).count() == 3

    # Проверка данных в ClickHouse
    with EventLogClient.init() as client:
        result = client.query("SELECT * FROM default.event_logs_test")
        assert len(result) == 3
        assert json.loads(result[0][3]) == {'data': 'test0'}

@pytest.mark.django_db
def test_retry_failed_events():
    # Создание записи
    log = Outbox.objects.create(
        event_type='TestFailure',
        environment='test',
        event_context={'error': 'test'},
        status=Outbox.STATUS_FAILED,
        metadata_version=1
    )

    process_outbox()
    log.refresh_from_db()

    # Проверка статуса
    assert log.status == Outbox.STATUS_PROCESSED

    # Проверка данных
    with EventLogClient.init() as client:
        result = client.query(
            "SELECT event_type FROM default.event_logs_test "
            "WHERE event_type = 'test_failure'"
        )
        assert len(result) == 1


