"""The identity of the container this runner is the main process of.

The runner's Redis claim never expires, so a crash leaves it behind and every
restart is refused until an operator clears it. One case is provably safe to
recover from without one: the same container restarting. A container's PID
namespace is destroyed when its PID 1 exits, so a process that *is* PID 1 knows
every earlier process of that container is gone — a claim carrying this
container's token cannot belong to a runner that is still alive, or paused and
about to resume.

The token is a random value kept on the container's writable layer. It survives
a restart, is lost when the container is recreated, and no other container can
read it. Anywhere the argument does not hold — a host process, ``docker exec``,
an init wrapper as PID 1, an unwritable path — there is no token, and the
runner falls back to refusing a held claim.
"""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from qte_shared.logging_setup import get_logger

log = get_logger(__name__)


def container_instance_token(instance_file: str) -> str | None:
    """Return this container's stable token, or ``None`` when it has none."""
    if os.getpid() != 1:
        return None
    instance_path = Path(instance_file)
    try:
        try:
            # Exclusive create: a file already there is the earlier process's.
            descriptor = os.open(instance_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            instance_token = instance_path.read_text(encoding="utf-8").strip()
        else:
            instance_token = uuid4().hex
            with os.fdopen(descriptor, "w", encoding="utf-8") as instance_stream:
                instance_stream.write(instance_token)
    except OSError as exc:
        log.warning("No container instance token at %s: %s", instance_path, exc)
        return None
    if not instance_token:
        log.warning("Container instance token at %s is empty", instance_path)
        return None
    return instance_token
