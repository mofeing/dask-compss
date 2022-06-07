from concurrent.futures import Executor
import functools
from typing import Callable, ParamSpec, TypeVar
from types import FunctionType

from pycompss import Future
from pycompss.api.task import task

T = TypeVar("T")
P = ParamSpec("P")

_RETURNS: dict[str | Callable, int | Callable] = {}


class COMPSsExecutor(Executor):
    def submit(self, fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        info = {"returns": _RETURNS[fn]} if fn in _RETURNS else {}
        task = self.generate_task(fn, **info)
        return task(*args, **kwargs)

    @functools.lru_cache
    def generate_task(fn, **info):
        return task(**info)(fn)
