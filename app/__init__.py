"""Macroeconomic news pipeline: extract → filter → classify → log → save."""

def run_pipeline():
    """
    Lazy wrapper to avoid package-level circular imports during startup.
    """
    from app.pipeline.run import run_pipeline as _run_pipeline

    return _run_pipeline()

__all__ = ["run_pipeline"]
