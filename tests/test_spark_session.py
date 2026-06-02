import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.spark_session import spark_runtime_allowed


def test_windows_auto_mode_prefers_pandas(monkeypatch):
    monkeypatch.delenv("SUBSCRIPTION_PIPELINE_ENGINE", raising=False)
    monkeypatch.setattr(sys, "platform", "win32")

    assert spark_runtime_allowed(log_reason=False) is False


def test_windows_spark_can_be_forced(monkeypatch):
    monkeypatch.setenv("SUBSCRIPTION_PIPELINE_ENGINE", "spark")
    monkeypatch.setattr(sys, "platform", "win32")

    assert spark_runtime_allowed(log_reason=False) is True


def test_pandas_engine_disables_spark(monkeypatch):
    monkeypatch.setenv("SUBSCRIPTION_PIPELINE_ENGINE", "pandas")
    monkeypatch.setattr(sys, "platform", "linux")

    assert spark_runtime_allowed(log_reason=False) is False
