from typing import Any, Dict, Optional, Protocol
import structlog
from django.db import transaction
from core.base_model import Model
from django.utils import timezone
from users.models import Outbox

logger = structlog.get_logger(__name__)

class UseCaseRequest(Model):
    pass


class UseCaseResponse(Model):
    result: Any = None
    error: str = ''


class UseCase(Protocol):
    def execute(self, request: UseCaseRequest) -> UseCaseResponse:
        with structlog.contextvars.bound_contextvars(
            **self._get_context_vars(request),
        ):
            return self._execute(request)

    def _get_context_vars(self, request: UseCaseRequest) -> dict[str, Any]:  # noqa: ARG002
        """
        !!! WARNING:
            This method is calling out of transaction so do not make db
            queries in this method.
        """
        return {
            'use_case': self.__class__.__name__,
        }

    @transaction.atomic()
    def _execute(self, request: UseCaseRequest) -> UseCaseResponse:
        raise NotImplementedError()


def log_event(event_type: str, event_context: dict[str, Any], environment: str) -> Optional[Outbox]:
    """Логирует событие в Outbox в рамках атомарной транзакции.

    Args:
        event_type (str): Тип события.
        event_context (dict[str, Any]): Контекст события (произвольные данные).
        environment (str): Среда выполнения.

    Returns:
        Optional[Outbox]: Объект Outbox, если успешно создан, иначе None.
    """
    try:
        with transaction.atomic():
            log_entry = Outbox.objects.create(
                event_type=event_type,
                event_date_time=timezone.now(),
                environment=environment,
                event_context=event_context,
                metadata_version=1,
                status=Outbox.STATUS_PENDING  # Устанавливаем статус "ожидание обработки"
            )
            return log_entry

    except Exception as e:
        logger.error("Failed to log event", error=str(e), event_type=event_type)
        return None