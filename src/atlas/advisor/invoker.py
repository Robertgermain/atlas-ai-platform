"""Live advisory structured invoker. No model-invocation ledger.

Malformed structured output is retried at most twice. Timeout, rate-limit,
auth, refusal, and unknown failures are not retried. The whole-analysis
monotonic deadline covers the complete attempt sequence. Metrics observed
here are process-local and disappear when the CLI exits.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from atlas.advisor.catalogs import (
    ADVISORY_ANALYSIS_DEADLINE_MARGIN_SECONDS,
    ADVISORY_NODE_NAME,
    ADVISORY_PROMPT_VERSION,
    FROZEN_LIVE_ADVISORY_MODEL,
    MALFORMED_ATTEMPT_CAP,
)
from atlas.advisor.contracts import AdvisoryAnalysis, AdvisoryIncidentFacts
from atlas.advisor.errors import AdvisoryAnalysisTimeoutError
from atlas.advisor.output_policy import validate_advisory_output
from atlas.advisor.prompt import render_advisory_prompts
from atlas.models.contracts import ModelCallMeta, ProviderId, RetryClass
from atlas.models.errors import ModelError, ModelInvalidStructuredOutputError
from atlas.models.langchain import invoke_structured
from atlas.observability.langsmith.client import current_langsmith
from atlas.observability.langsmith.redaction import filter_metadata
from atlas.observability.langsmith.tracing import (
    attach_run_metadata,
    run_in_tracing_context,
    trace_ai,
)
from atlas.observability.metrics import AtlasMetrics, default_metrics

_tracer = trace.get_tracer(__name__)


class AdvisoryStructuredInvoker:
    """Live LangChain structured output without ModelInvocationService."""

    def __init__(
        self,
        *,
        chat_model: object,
        provider: ProviderId,
        model_name: str,
        call_timeout_seconds: float,
        metrics: AtlasMetrics | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._chat_model = chat_model
        self._provider = provider
        self._model_name = model_name
        self._call_timeout_seconds = call_timeout_seconds
        self._metrics = metrics if metrics is not None else default_metrics()
        self._monotonic = monotonic

    def analyze(
        self,
        facts: AdvisoryIncidentFacts,
        *,
        analysis_id: str | None = None,
    ) -> AdvisoryAnalysis:
        deadline = self._monotonic() + (
            MALFORMED_ATTEMPT_CAP * self._call_timeout_seconds
            + ADVISORY_ANALYSIS_DEADLINE_MARGIN_SECONDS
        )
        system_prompt, user_prompt = render_advisory_prompts(facts)
        metadata = {
            "atlas.node_name": ADVISORY_NODE_NAME,
            "atlas.prompt_version": ADVISORY_PROMPT_VERSION,
            "atlas.model.provider": self._provider.value,
            "atlas.model_name": self._model_name,
            "atlas.research_job_id": facts.research_job_id,
        }
        if analysis_id:
            metadata["atlas.advisory_analysis_id"] = analysis_id

        def _run() -> AdvisoryAnalysis:
            return self._attempt_sequence(
                facts, system_prompt, user_prompt, deadline=deadline
            )

        def _run_traced() -> AdvisoryAnalysis:
            with _tracer.start_as_current_span(
                "atlas.advisor.analyze",
                attributes={
                    "atlas.node_name": ADVISORY_NODE_NAME,
                    "atlas.research_job_id": facts.research_job_id,
                },
            ) as span:
                try:
                    return trace_ai(
                        name="atlas.advisor",
                        run_type="chain",
                        fn=lambda: trace_ai(
                            name=ADVISORY_NODE_NAME,
                            run_type="llm",
                            fn=_run,
                            metadata=metadata,
                        ),
                        metadata=metadata,
                    )
                except Exception as exc:
                    span.set_status(Status(StatusCode.ERROR))
                    span.set_attribute("error.class", exc.__class__.__name__)
                    raise

        handle = current_langsmith()
        if handle.enabled and handle.client is not None:
            return run_in_tracing_context(
                client=handle.client,
                project=handle.project,
                metadata=filter_metadata(metadata),
                fn=_run_traced,
            )
        return _run_traced()

    def _attempt_sequence(
        self,
        facts: AdvisoryIncidentFacts,
        system_prompt: str,
        user_prompt: str,
        *,
        deadline: float,
    ) -> AdvisoryAnalysis:
        last_malformed: ModelInvalidStructuredOutputError | None = None
        for attempt in range(1, MALFORMED_ATTEMPT_CAP + 1):
            if self._monotonic() >= deadline:
                self._observe_logical("failed")
                raise AdvisoryAnalysisTimeoutError()
            started = self._monotonic()
            try:
                parsed, meta = invoke_structured(
                    chat_model=self._chat_model,  # type: ignore[arg-type]
                    provider=self._provider,
                    model_name=self._model_name,
                    prompt_version=ADVISORY_PROMPT_VERSION,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    schema=AdvisoryAnalysis,
                )
                validate_advisory_output(facts, parsed)
            except ModelInvalidStructuredOutputError as exc:
                duration = self._monotonic() - started
                self._observe_attempt(
                    outcome="failed",
                    retry_class=RetryClass.INVALID_STRUCTURED_OUTPUT.value,
                    duration_seconds=duration,
                )
                last_malformed = exc
                if attempt >= MALFORMED_ATTEMPT_CAP or self._monotonic() >= deadline:
                    self._observe_logical("failed")
                    if self._monotonic() >= deadline:
                        raise AdvisoryAnalysisTimeoutError() from exc
                    raise
                continue
            except AdvisoryAnalysisTimeoutError:
                raise
            except ModelError as exc:
                duration = self._monotonic() - started
                self._observe_attempt(
                    outcome="failed",
                    retry_class=exc.retry_class.value,
                    duration_seconds=duration,
                )
                self._observe_logical("failed")
                raise
            except Exception:
                duration = self._monotonic() - started
                self._observe_attempt(
                    outcome="failed",
                    retry_class=RetryClass.UNKNOWN.value,
                    duration_seconds=duration,
                )
                self._observe_logical("failed")
                raise
            duration = self._monotonic() - started
            self._observe_attempt(
                outcome="succeeded",
                retry_class=RetryClass.NONE.value,
                duration_seconds=duration,
            )
            self._observe_logical("succeeded")
            self._observe_usage(meta)
            attach_run_metadata(
                {
                    "atlas.node_name": ADVISORY_NODE_NAME,
                    "atlas.prompt_version": ADVISORY_PROMPT_VERSION,
                    "atlas.model.provider": self._provider.value,
                    "atlas.model_name": self._model_name
                    if self._model_name == FROZEN_LIVE_ADVISORY_MODEL
                    else FROZEN_LIVE_ADVISORY_MODEL,
                }
            )
            return parsed
        self._observe_logical("failed")
        if last_malformed is not None:
            raise last_malformed
        raise AdvisoryAnalysisTimeoutError()

    def _observe_attempt(
        self, *, outcome: str, retry_class: str, duration_seconds: float
    ) -> None:
        self._metrics.observe_model_attempt(
            node_name=ADVISORY_NODE_NAME,
            provider=self._provider.value,
            outcome=outcome,
            retry_class=retry_class,
            duration_seconds=duration_seconds,
        )

    def _observe_logical(self, outcome: str) -> None:
        self._metrics.observe_model_invocation(
            node_name=ADVISORY_NODE_NAME,
            provider=self._provider.value,
            outcome=outcome,
        )

    def _observe_usage(self, meta: ModelCallMeta) -> None:
        self._metrics.observe_model_tokens(
            node_name=ADVISORY_NODE_NAME,
            provider=self._provider.value,
            input_tokens=meta.input_tokens,
            output_tokens=meta.output_tokens,
        )
        self._metrics.observe_model_cost(
            node_name=ADVISORY_NODE_NAME,
            provider=self._provider.value,
            cost_usd=meta.estimated_cost_usd,
        )
