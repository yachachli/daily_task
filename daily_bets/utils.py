import asyncio
import typing as t
from itertools import batched

from neverraise import Result, ResultAsync


async def batch_calls_result_async[T, E](
    datas: t.Iterable[t.Any],
    func: t.Callable[..., ResultAsync[T, E]],
    batch_size: int,
) -> list[Result[T, E]]:
    results: list[Result[T, E]] = []
    for chunk in batched(datas, batch_size):
        results.extend(await asyncio.gather(*(func(*params) for params in chunk)))
    return results


def normalize_name(name: str) -> str:
    return name.strip().lower().replace(".", "")
