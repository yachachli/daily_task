import asyncio
import json
import typing as t
from dataclasses import asdict
from itertools import batched


class DataclassEncoder(json.JSONEncoder):
    def default(self, o: t.Any):
        if hasattr(o, "__dataclass_fields__"):  # checks if the object is a dataclass
            return asdict(o)  # convert dataclass to dict
        return super().default(o)


def json_dumps_dataclass(obj: t.Any):
    """Turns any class decorated with `@dataclass` into json."""
    return json.dumps(obj, cls=DataclassEncoder, indent=4)


T = t.TypeVar("T")
R = t.TypeVar("R")


async def batch_calls(
    datas: t.Iterable[T],
    func: t.Callable[..., t.Awaitable[R]],
    batch_size: int,
) -> list[R | Exception]:
    """Takes an `Iterable` (like a list) and a function `func` that takes the item of the iterable as an arguemnt, batches the calls, and returns the results.

    Any exceptions raised in the function will be returned as values.
    """
    results: list[R | Exception] = []
    for chunk in batched(datas, batch_size):
        results.extend(
            await asyncio.gather(
                *(func(params) for params in chunk), return_exceptions=True
            )  # type: ignore Wants BaseException but doesn't work with Exception for some reason
        )
    return results
