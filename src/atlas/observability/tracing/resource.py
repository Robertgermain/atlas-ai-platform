"""Fixed OpenTelemetry resource identity, set at the SDK layer (Slice 15A3).

Each process sets its own ``service.name`` explicitly at
:class:`~opentelemetry.sdk.resources.Resource` construction -- Atlas never
relies on the Collector to distinguish producers by connection origin or
any other enrichment. ``service.namespace``/``deployment.environment`` are
likewise fixed here, not left to Collector-side processors.
"""

from __future__ import annotations

from typing import Final, Literal

from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes

#: The complete, closed set of Atlas process roles that construct a
#: tracing resource. Matches (a superset check would be redundant with)
#: ``atlas.observability.logging``'s own ``_KNOWN_SERVICE_ROLES`` list --
#: kept as an independent literal here because a resource's ``service.name``
#: uses the ``atlas-<role>`` OpenTelemetry semantic-convention style
#: (hyphenated, prefixed) rather than the shorter logging ``service_role``
#: string.
ServiceName = Literal[
    "atlas-api",
    "atlas-worker",
    "atlas-outbox-relay",
    "atlas-consumer",
    "atlas-advisor",
]

#: Fixed, bounded set -- never derived from arbitrary environment content.
DeploymentEnvironment = Literal["local", "kind", "aws"]

_SERVICE_NAMESPACE: Final[str] = "atlas"


def build_resource(
    *, service_name: ServiceName, deployment_environment: DeploymentEnvironment
) -> Resource:
    """Build this process's fixed OpenTelemetry resource.

    ``Resource.create`` also adds the SDK's own standard attributes
    (``telemetry.sdk.name``, ``telemetry.sdk.language``,
    ``telemetry.sdk.version``, a generated ``service.instance.id``, etc.) --
    this is expected and is not filtered out; only the three Atlas-provided
    attributes below are ever set explicitly by this function.
    """
    return Resource.create(
        {
            ResourceAttributes.SERVICE_NAME: service_name,
            ResourceAttributes.SERVICE_NAMESPACE: _SERVICE_NAMESPACE,
            ResourceAttributes.DEPLOYMENT_ENVIRONMENT: deployment_environment,
        }
    )
