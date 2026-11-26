import asyncio
import sys
from collections.abc import Coroutine
from typing import Any


def run[T](main: Coroutine[Any, Any, T]) -> T:
    if sys.platform == "win32":
        return asyncio.run(main, loop_factory=asyncio.SelectorEventLoop)
    return asyncio.run(main)
