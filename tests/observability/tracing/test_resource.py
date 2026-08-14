"""Fixed OpenTelemetry resource identity (Slice 15A3 final condition #4).

``Resource.create()`` also attaches the SDK's own standard attributes
(``telemetry.sdk.name``/``telemetry.sdk.language``/``telemetry.sdk.version``,
a generated ``service.instance.id``, etc.) -- these tests assert the three
required fixed Atlas attributes are present with the exact given values,
and separately assert that no *other* Atlas-provided (i.e. non-SDK-standard)
attribute exists, without requiring the whole resource dict to contain only
three keys.
"""

from __future__ import annotations

import pytest
from opentelemetry.semconv.resource import ResourceAttributes

from atlas.observability.tracing.resource import ServiceName, build_resource

#: The SDK's own standard attributes that `Resource.create()` always adds on
#: top of whatever explicit attributes are passed in -- not Atlas-provided,
#: so their presence (with SDK-chosen values) is expected and never
#: asserted-against as a "prohibited" attribute.
_SDK_STANDARD_ATTRIBUTE_KEYS = frozenset(
    {
        ResourceAttributes.TELEMETRY_SDK_NAME,
        ResourceAttributes.TELEMETRY_SDK_LANGUAGE,
        ResourceAttributes.TELEMETRY_SDK_VERSION,
        ResourceAttributes.SERVICE_INSTANCE_ID,
    }
)

_ALL_SERVICE_NAMES: tuple[ServiceName, ...] = (
    "atlas-api",
    "atlas-worker",
    "atlas-outbox-relay",
    "atlas-consumer",
    "atlas-advisor",
)


@pytest.mark.parametrize("service_name", _ALL_SERVICE_NAMES)
def test_build_resource_sets_exactly_the_three_fixed_atlas_attributes(
    service_name: ServiceName,
) -> None:
    resource = build_resource(service_name=service_name, deployment_environment="local")
    attributes = dict(resource.attributes)

    assert attributes[ResourceAttributes.SERVICE_NAME] == service_name
    assert attributes[ResourceAttributes.SERVICE_NAMESPACE] == "atlas"
    assert attributes[ResourceAttributes.DEPLOYMENT_ENVIRONMENT] == "local"


@pytest.mark.parametrize("deployment_environment", ["local", "kind", "aws"])
def test_build_resource_accepts_every_bounded_deployment_environment(
    deployment_environment: str,
) -> None:
    resource = build_resource(
        service_name="atlas-api",
        deployment_environment=deployment_environment,  # type: ignore[arg-type]
    )
    attributes = dict(resource.attributes)
    assert attributes[ResourceAttributes.DEPLOYMENT_ENVIRONMENT] == (
        deployment_environment
    )


def test_build_resource_never_sets_an_atlas_attribute_outside_the_fixed_three() -> None:
    """Only the SDK's own standard keys may exist alongside the fixed three.

    This is the "reject prohibited/unbounded Atlas-provided attributes"
    requirement: :func:`build_resource`'s signature only accepts the three
    fixed, bounded parameters below (enforced statically by its
    ``Literal``-typed keyword arguments -- there is no way to pass an
    arbitrary extra attribute at all), so this test instead proves the
    *resulting* resource dictionary contains no additional Atlas-namespaced
    key beyond the three fixed ones plus the SDK's own standard metadata.
    """
    resource = build_resource(service_name="atlas-api", deployment_environment="local")
    attributes = dict(resource.attributes)

    fixed_atlas_keys = {
        ResourceAttributes.SERVICE_NAME,
        ResourceAttributes.SERVICE_NAMESPACE,
        ResourceAttributes.DEPLOYMENT_ENVIRONMENT,
    }
    unexpected_keys = (
        set(attributes.keys()) - fixed_atlas_keys - _SDK_STANDARD_ATTRIBUTE_KEYS
    )
    assert unexpected_keys == set()


def test_build_resource_service_names_are_distinct_across_roles() -> None:
    resources = {
        service_name: dict(
            build_resource(
                service_name=service_name, deployment_environment="local"
            ).attributes
        )[ResourceAttributes.SERVICE_NAME]
        for service_name in _ALL_SERVICE_NAMES
    }
    assert len(set(resources.values())) == len(_ALL_SERVICE_NAMES)
