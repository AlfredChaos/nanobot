"""Context-compaction lifecycle notices are delivered regardless of ``send_progress``.

Compaction changes the context of every later turn, so both the start and
the outcome of a compaction are information for the user, not progress
chatter: a channel with ``send_progress`` off still receives them. Reducing
the noise (one message updated in place) is the adapter's job; see the
Discord channel (#5719). Channels whose transport cannot update a message
in place (QQ: no edit/recall endpoint for C2C/group) set
``show_compaction_notices = False`` and drop the notices instead (#5784).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from nanobot.bus.events import OutboundMessage
from nanobot.bus.outbound_events import (
    ContextCompactionEvent,
    ProgressEvent,
    outbound_message_for_event,
)
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.channels.manager import ChannelManager
from nanobot.config.schema import Config


class _MockChannel(BaseChannel):
    name = "mock"
    display_name = "Mock"

    def __init__(self, config, bus):
        super().__init__(config, bus)
        self._send_mock = AsyncMock()

    async def start(self):  # pragma: no cover - not exercised
        pass

    async def stop(self):  # pragma: no cover - not exercised
        pass

    async def send(self, msg):
        return await self._send_mock(msg)


@pytest.fixture
def manager() -> ChannelManager:
    config = Config.model_validate({"channels": {"websocket": {"enabled": False}}})
    mgr = ChannelManager(config, MessageBus())
    mgr.channels["mock"] = _MockChannel({}, mgr.bus)
    return mgr


async def _dispatch_until(manager: ChannelManager, expected: int) -> None:
    task = asyncio.create_task(manager._dispatch_outbound())
    try:
        for _ in range(40):
            if manager.channels["mock"]._send_mock.await_count >= expected:
                break
            await asyncio.sleep(0.05)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def _sent_contents(manager: ChannelManager) -> list[str]:
    return [call.args[0].content for call in manager.channels["mock"]._send_mock.await_args_list]


@pytest.mark.asyncio
async def test_compaction_lifecycle_is_delivered_with_progress_off(manager: ChannelManager) -> None:
    manager.channels["mock"].send_progress = False
    for event in (
        ProgressEvent(content="ordinary progress"),
        ContextCompactionEvent(compaction_id="c1", phase="started"),
        ContextCompactionEvent(compaction_id="c1", phase="succeeded"),
    ):
        await manager.bus.publish_outbound(
            outbound_message_for_event(channel="mock", chat_id="chat", event=event)
        )

    await _dispatch_until(manager, 2)

    contents = _sent_contents(manager)
    assert "ordinary progress" not in contents
    assert len(contents) == 2


@pytest.mark.asyncio
async def test_compaction_notices_dropped_when_capability_off(manager: ChannelManager) -> None:
    manager.channels["mock"].show_compaction_notices = False
    await manager.bus.publish_outbound(
        OutboundMessage(channel="mock", chat_id="chat", content="ordinary reply")
    )
    for phase in ("started", "succeeded"):
        await manager.bus.publish_outbound(
            outbound_message_for_event(
                channel="mock",
                chat_id="chat",
                event=ContextCompactionEvent(compaction_id="c1", phase=phase),
            )
        )

    await _dispatch_until(manager, 1)

    contents = _sent_contents(manager)
    assert contents == ["ordinary reply"]


def test_qq_channel_opts_out_of_compaction_notices() -> None:
    from nanobot.channels.qq.runtime import QQChannel

    assert QQChannel.show_compaction_notices is False


def test_compaction_notice_capability_is_config_overridable(manager: ChannelManager) -> None:
    assert manager._resolve_bool_override(
        {"showCompactionNotices": True}, "show_compaction_notices", default=False,
    )
    assert not manager._resolve_bool_override(
        {"sendProgress": True}, "show_compaction_notices", default=False,
    )
    assert manager._resolve_bool_override(
        {}, "show_compaction_notices", default=True,
    )
