from __future__ import annotations

import sys
import threading
import time
from collections.abc import Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any

from rich.console import Console
from rich.progress import BarColumn, Progress, TaskID, TextColumn

_CURRENT_REPORTER: ContextVar[Any] = ContextVar(
    "centric_mdm_progress_reporter",
    default=None,
)
LABEL_WIDTH = 44


@dataclass(frozen=True)
class ProgressEvent:
    stage: str
    action: str
    message: str = ""
    current: int | None = None
    total: int | None = None
    unit: str = ""
    overall_increment: float | None = None


class ProgressReporter:
    """Render chronological CLI progress with one active progress bar at a time."""

    def __init__(self, *, enabled: bool | None = None) -> None:
        self.enabled = sys.stderr.isatty() if enabled is None else enabled
        self._console = Console(stderr=True, highlight=False)
        self._context_token: Token[ProgressReporter | None] | None = None
        self._progress: Progress | None = None
        self._active_task_id: TaskID | None = None
        self._overall_task_id: TaskID | None = None
        self._active_stage: str | None = None
        self._active_total: int | None = None
        self._stage_stack: list[str] = []
        self._overall_label: str | None = None
        self._overall_progress = 0.0
        self._overall_stage_start = 0.0
        self._overall_stage_weight = 0.0
        self._overall_stage_progress = 0.0
        self._overall_stage_started_at: float | None = None
        self._overall_stage_estimate: float | None = None
        self._last_overall_percent: int | None = None
        self._started_at: dict[str, float] = {}
        self._lock = threading.Lock()
        self._ticker_stop = threading.Event()
        self._ticker: threading.Thread | None = None

    def __enter__(self) -> ProgressReporter:
        if self.enabled:
            self._context_token = _CURRENT_REPORTER.set(self)
            self._progress = Progress(
                TextColumn("      {task.description}"),
                BarColumn(bar_width=24, complete_style="green", finished_style="green"),
                TextColumn("{task.fields[detail]}"),
                console=self._console,
                transient=True,
            )
            self._progress.start()
            self._ticker_stop.clear()
            self._ticker = threading.Thread(target=self._tick_overall, daemon=True)
            self._ticker.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._ticker_stop.set()
        if self._ticker is not None:
            self._ticker.join(timeout=1)
        self._close_active_task()
        if self._progress is not None:
            self._progress.stop()
        self._progress = None
        if self._context_token is not None:
            _CURRENT_REPORTER.reset(self._context_token)

    def __call__(self, event: ProgressEvent | Mapping[str, Any]) -> None:
        if not self.enabled:
            return
        event = _coerce_event(event)
        if event.action == "message":
            self.print(event.message or event.stage)
            return
        if event.action == "start":
            self._start(event)
            return
        if event.action == "advance":
            self._update(
                ProgressEvent(
                    stage=event.stage,
                    action="update",
                    message=event.message,
                    current=event.current,
                    total=event.total,
                    unit=event.unit,
                )
            )
            return
        if event.action == "update":
            self._update(event)
            return
        if event.action == "finish":
            self._finish(event)

    def emit(
        self,
        stage: str,
        action: str,
        *,
        message: str = "",
        current: int | None = None,
        total: int | None = None,
        unit: str = "",
    ) -> None:
        self(
            ProgressEvent(
                stage=stage,
                action=action,
                message=message,
                current=current,
                total=total,
                unit=unit,
            )
        )

    def start_overall(self, label: str = "Overall") -> None:
        if not self.enabled:
            return
        self._overall_label = label
        self._overall_progress = 0.0
        self._overall_stage_start = 0.0
        self._overall_stage_weight = 0.0
        self._overall_stage_progress = 0.0
        self._last_overall_percent = None
        progress = self._progress
        if progress is None:
            return
        self._add_overall_task()

    def begin_overall_stage(self, weight: float, *, estimated_seconds: float | None = None) -> None:
        if not self.enabled or self._overall_label is None:
            return
        with self._lock:
            self._overall_stage_start = self._overall_progress
            self._overall_stage_weight = max(0.0, weight)
            self._overall_stage_progress = 0.0
            self._overall_stage_started_at = time.perf_counter()
            self._overall_stage_estimate = estimated_seconds

    def advance_overall_stage(self, fraction: float) -> None:
        if not self.enabled or self._overall_label is None:
            return
        with self._lock:
            self._advance_overall_stage_locked(
                min(
                    1.0,
                    max(self._overall_stage_progress, self._overall_stage_progress + fraction),
                )
            )
        self._update_overall_task()

    def finish_overall_stage(self) -> None:
        if not self.enabled or self._overall_label is None:
            return
        with self._lock:
            self._advance_overall_stage_locked(1.0)
            self._overall_stage_started_at = None
            self._overall_stage_estimate = None
        self._update_overall_task()

    def finish_overall(self) -> None:
        if not self.enabled or self._overall_label is None:
            return
        if self._overall_progress >= 0.999:
            return
        with self._lock:
            self._overall_progress = 1.0
            self._overall_stage_started_at = None
            self._overall_stage_estimate = None
        self._update_overall_task()

    def section(self, title: str) -> None:
        if self.enabled:
            self._console.print()
            self._console.print(f"[bold]{title}[/bold]")

    def print(self, message: str) -> None:
        if self.enabled:
            self._console.print(message)

    def _start(self, event: ProgressEvent) -> None:
        self._started_at[event.stage] = time.perf_counter()
        self._push_stage(event.stage)
        self._close_active_task()
        progress = self._progress
        if progress is None:
            return
        self._remove_overall_task()
        total = event.total if _should_show_bar(event) else None
        self._active_task_id = progress.add_task(
            _label(self._stage_path()),
            total=total,
            completed=event.current or 0,
            detail=_task_detail(event.current or 0, total, event.unit),
        )
        self._add_overall_task()
        self._active_stage = event.stage
        self._active_total = total

    def _update(self, event: ProgressEvent) -> None:
        if self._progress is None or self._active_task_id is None:
            self._advance_overall_from_event(event)
            return
        if self._active_stage != event.stage:
            self._finish(
                ProgressEvent(
                    stage=self._active_stage or event.stage,
                    action="finish",
                    message="done",
                    total=self._active_total,
                )
            )
            self._start(event)
            return
        update_kwargs: dict[str, Any] = {}
        total = self._active_total
        if event.current is not None:
            update_kwargs["completed"] = event.current
        if event.total is not None:
            total = event.total if _should_show_bar(event) else None
            update_kwargs["total"] = total
            self._active_total = total
        current = event.current
        update_kwargs["detail"] = _task_detail(current or 0, total, event.unit)
        self._progress.update(self._active_task_id, **update_kwargs)
        self._advance_overall_from_event(event)

    def _finish(self, event: ProgressEvent) -> None:
        elapsed = self._elapsed(event.stage)
        if self._progress is not None and self._active_task_id is not None:
            total = event.total if event.total is not None else self._active_total
            if total is not None:
                self._progress.update(self._active_task_id, completed=total, total=total)
            self._close_active_task()
            self._console.print(
                _done_line(event.stage, event.message, total=total, elapsed=elapsed)
            )
            self._pop_stage(event.stage)
            self._advance_overall_from_event(event)
            return
        self._console.print(
            _done_line(event.stage, event.message, total=event.total, elapsed=elapsed)
        )
        self._pop_stage(event.stage)
        self._advance_overall_from_event(event)

    def _close_active_task(self) -> None:
        if self._progress is not None and self._active_task_id is not None:
            self._progress.remove_task(self._active_task_id)
        self._active_task_id = None
        self._active_stage = None
        self._active_total = None

    def _remove_overall_task(self) -> None:
        if self._progress is not None and self._overall_task_id is not None:
            self._progress.remove_task(self._overall_task_id)
        self._overall_task_id = None

    def _add_overall_task(self) -> None:
        if self._progress is None or self._overall_label is None:
            return
        percent = round(self._overall_progress * 100)
        self._overall_task_id = self._progress.add_task(
            _label(self._overall_label),
            total=100,
            completed=percent,
            detail=f"{percent}%",
        )

    def _push_stage(self, stage: str) -> None:
        if not self._stage_stack or self._stage_stack[-1] != stage:
            self._stage_stack.append(stage)

    def _pop_stage(self, stage: str) -> None:
        for index in range(len(self._stage_stack) - 1, -1, -1):
            if self._stage_stack[index] == stage:
                del self._stage_stack[index:]
                return

    def _stage_path(self, *, leaf: str | None = None) -> str:
        stages = list(self._stage_stack)
        if leaf is not None and leaf not in stages:
            stages.append(leaf)
        return " > ".join(_compact_stage_name(stage) for stage in stages[-3:])

    def _advance_overall_from_event(self, event: ProgressEvent) -> None:
        if event.overall_increment is not None:
            self.advance_overall_stage(event.overall_increment)

    def _advance_overall_stage_locked(self, stage_progress: float) -> None:
        self._overall_stage_progress = min(
            1.0,
            max(self._overall_stage_progress, stage_progress),
        )
        self._overall_progress = min(
            1.0,
            self._overall_stage_start
            + (self._overall_stage_weight * self._overall_stage_progress),
        )

    def _tick_overall(self) -> None:
        while not self._ticker_stop.wait(0.2):
            if not self.enabled or self._overall_label is None:
                continue
            with self._lock:
                if not self._overall_stage_started_at or not self._overall_stage_estimate:
                    continue
                if self._overall_stage_estimate <= 0:
                    continue
                elapsed = time.perf_counter() - self._overall_stage_started_at
                stage_progress = min(1.0, elapsed / self._overall_stage_estimate)
                previous_percent = round(self._overall_progress * 100)
                self._advance_overall_stage_locked(stage_progress)
                next_percent = round(self._overall_progress * 100)
            if next_percent != previous_percent:
                self._update_overall_task()

    def _update_overall_task(self) -> None:
        if self._progress is None or self._overall_task_id is None:
            return
        percent = round(self._overall_progress * 100)
        if percent == self._last_overall_percent:
            return
        self._last_overall_percent = percent
        self._progress.update(
            self._overall_task_id,
            completed=percent,
            detail=f"{percent}%",
        )

    def _elapsed(self, stage: str) -> float | None:
        started_at = self._started_at.pop(stage, None)
        if started_at is None:
            return None
        return time.perf_counter() - started_at


def progress_enabled() -> bool:
    reporter = _CURRENT_REPORTER.get()
    return bool(reporter and reporter.enabled)


def progress_section(title: str) -> None:
    reporter = _CURRENT_REPORTER.get()
    if reporter and reporter.enabled:
        reporter.section(title)


def progress_message(message: str) -> None:
    reporter = _CURRENT_REPORTER.get()
    if reporter and reporter.enabled:
        reporter.print(message)


def _should_show_bar(event: ProgressEvent) -> bool:
    if event.total is None or event.total <= 0:
        return False
    if event.total >= 10:
        return True
    stage = event.stage.lower()
    return any(term in stage for term in ("style", "product", "record", "row", "file"))


def _task_detail(current: int, total: int | None, unit: str) -> str:
    if total is None:
        return ""
    suffix = f" {unit}" if unit else ""
    return f"{current}/{total}{suffix}"


def _compact_stage_name(stage: str) -> str:
    replacements = (
        ("Writing DPP reports", "Reports"),
        ("Writing MD reports", "Reports"),
        ("Building DPP issue workbook", "Issue workbook"),
        ("Building MD issue workbook", "Issue workbook"),
        ("Building DPP summary workbook", "Summary workbook"),
        ("Building MD summary workbook", "Summary workbook"),
        ("Building MD season warnings", "Season warnings"),
        ("Building MD reference coverage", "Reference coverage"),
        ("Formatting DPP issue workbook", "Formatting"),
        ("Formatting MD issue workbook", "Formatting"),
        ("Formatting DPP summary workbook", "Formatting"),
        ("Formatting MD summary workbook", "Formatting"),
        ("Formatting MD season warnings", "Formatting"),
        ("Formatting MD reference coverage", "Formatting"),
        ("Saving DPP issue workbook", "Saving"),
        ("Saving MD issue workbook", "Saving"),
        ("Saving DPP summary workbook", "Saving"),
        ("Saving MD summary workbook", "Saving"),
        ("Saving MD season warnings", "Saving"),
        ("Saving MD reference coverage", "Saving"),
        ("Writing DPP summary markdown", "Summary markdown"),
        ("Writing MD summary markdown", "Summary markdown"),
    )
    for full_name, compact_name in replacements:
        if stage == full_name:
            return compact_name
    return stage


def _label(value: str) -> str:
    value = " ".join(str(value).split())
    if len(value) > LABEL_WIDTH:
        value = value[: LABEL_WIDTH - 1] + "…"
    return f"{value:<{LABEL_WIDTH}}"


def _done_line(
    stage: str,
    message: str = "",
    *,
    total: int | None = None,
    elapsed: float | None = None,
) -> str:
    detail = message or (f"{total} done" if total is not None else "done")
    duration = f" in {_format_duration(elapsed)}" if elapsed is not None else ""
    if detail == "done":
        return f"      {_label(stage)} done{duration}"
    return f"      {_label(stage)} done{duration}: {detail}"


def _format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{round(seconds * 1000)}ms"
    return f"{seconds:.1f}s"


def _coerce_event(event: ProgressEvent | Mapping[str, Any]) -> ProgressEvent:
    if isinstance(event, ProgressEvent):
        return event
    return ProgressEvent(
        stage=str(event.get("stage") or "Progress"),
        action=str(event.get("action") or "message"),
        message=str(event.get("message") or ""),
        current=_optional_int(event.get("current")),
        total=_optional_int(event.get("total")),
        unit=str(event.get("unit") or ""),
        overall_increment=_optional_float(event.get("overall_increment")),
    )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
