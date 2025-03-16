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
    """Takes an `Iterable` of tuples (like a list of tuples) and a function `func` that takes the items of the tuple of the iterable as arguemnts, batches the calls, and returns the results.

    Any exceptions raised in the function will be returned as values.


    ```py
    lst = [(1, "a"), (2, "b")]

    async def func(num: int, letter: str):
        print(num, letter)

    batch_calls(lst, func, 2)

    ```


    ```py
    lst2 = [(1,), (2,), (3,), (4,)]

    async def func(num: int):
        print(num * 2)

    batch_calls(lst2, func, 2)
    ```
    """
    results: list[R | Exception] = []
    for chunk in batched(datas, batch_size):
        results.extend(
            await asyncio.gather(
                *(func(*params) for params in chunk), return_exceptions=True
            )  # type: ignore Wants BaseException but doesn't work with Exception for some reason
        )
    return results


def normalize_name(name: str) -> str:
    return name.strip().lower().replace(".", "")
