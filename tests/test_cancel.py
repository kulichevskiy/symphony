from __future__ import annotations

from types import SimpleNamespace

from symphony.cancel import is_issue_canceled, request_cancel
from symphony.events import EventLog


def test_request_cancel_marks_local_marker_labels_issue_and_logs(tmp_path):
    cfg = SimpleNamespace(repo=SimpleNamespace(path=tmp_path))
    labels = []

    def label_fn(issue_number, label, *, repo_path):
        labels.append((issue_number, label, repo_path))

    marker = request_cancel(
        cfg,
        42,
        label_fn=label_fn,
        event_log=EventLog.for_repo(tmp_path),
    )

    assert marker == tmp_path / ".symphony" / "canceled" / "42"
    assert is_issue_canceled(tmp_path, 42)
    assert labels == [(42, "auto-canceled", tmp_path)]
    events = EventLog.for_repo(tmp_path).iter_events(issue_number=42)
    assert [event.kind for event in events] == ["auto-canceled"]
