"""FastAPI application entrypoint."""

from fastapi import FastAPI

app = FastAPI(title="Atlas AI Platform", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    """Return service liveness status."""
    return {"status": "ok"}
