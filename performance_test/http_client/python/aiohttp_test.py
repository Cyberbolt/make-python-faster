import time
import asyncio
import collections

import uvloop
import aiohttp

URL = "http://nginx:21000"


async def fetch(session: aiohttp.ClientSession):
    """Performs a single HTTP GET request."""
    url = URL
    # The client session timeout will handle request timeouts.
    async with session.get(url) as response:
        # We don't need the body, but reading it ensures the connection
        # is released back to the pool properly.
        await response.read()


async def worker(
    session: aiohttp.ClientSession,
    counters: collections.Counter,
    start_event: asyncio.Event,
):
    """A worker that runs until cancelled, performing requests and counting results."""
    await start_event.wait()
    while True:
        try:
            await fetch(session)
            counters["success"] += 1
        except asyncio.CancelledError:
            # The task was cancelled, which is the signal to stop.
            break
        except Exception:
            # Any other exception is treated as a failure.
            counters["failed"] += 1


async def main():
    test_duration = 60  # seconds
    concurrency = 100
    counters = collections.Counter()
    start_event = asyncio.Event()

    # A timeout is set on the session, so individual requests will time out if they take too long.
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print(
            f"Starting throughput test for {test_duration}s with a concurrency of {concurrency}..."
        )

        # Create worker tasks. They will wait for the start_event.
        tasks = [
            asyncio.create_task(worker(session, counters, start_event))
            for _ in range(concurrency)
        ]

        # All tasks are created and waiting. Now, signal them to start and begin timing.
        start_event.set()
        start_time = time.monotonic()

        # Let the workers run for the specified duration.
        await asyncio.sleep(test_duration)

        # Cancel all worker tasks to stop them gracefully.
        for task in tasks:
            task.cancel()

        # Wait for all tasks to finish their cancellation.
        await asyncio.gather(*tasks, return_exceptions=True)

        end_time = time.monotonic()
        actual_duration = end_time - start_time

        print("\n--- Test Results ---")
        print(f"Test ran for: {actual_duration:.2f} seconds")
        print(f"Successful requests: {counters['success']}")
        print(f"Failed requests: {counters['failed']}")

        if actual_duration > 0:
            rps = counters["success"] / actual_duration
            print(f"Successful requests per second (RPS): {rps:.0f}")


if __name__ == "__main__":
    # asyncio.run(main())
    uvloop.run(main())
