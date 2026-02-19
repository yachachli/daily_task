import asyncio
import typing as t
from itertools import batched

from neverraise import Result, ResultAsync


async def batch_calls_result_async[T, E](
    datas: t.Iterable[t.Any],
    func: t.Callable[..., ResultAsync[T, E]],
    batch_size: int,
    delay_between_batches: float = 0.0,
) -> list[Result[T, E]]:
    results: list[Result[T, E]] = []
    chunks = list(batched(datas, batch_size))
    for i, chunk in enumerate(chunks):
        results.extend(await asyncio.gather(*(func(*params) for params in chunk)))
        # Add delay between batches (but not after the last batch)
        if delay_between_batches > 0 and i < len(chunks) - 1:
            await asyncio.sleep(delay_between_batches)
    return results


def normalize_name(name: str) -> str:
    return name.strip().lower().replace(".", "")
