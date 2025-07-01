import time
import asyncio
import collections

import uvloop
import aiohttp

URL = "http://192.168.31.233:21000"


async def fetch(session: aiohttp.ClientSession):
    """Performs a single HTTP GET request."""
    url = URL
    # The client session timeout will handle request timeouts.
    async with session.get(url) as response:
        # We don't need the body, but reading it ensures the connection
        # is released back to the pool properly.
        await response.read()


async def worker(session: aiohttp.ClientSession, counters: collections.Counter):
    """A worker that runs until cancelled, performing requests and counting results."""
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
    test_duration = 30  # seconds
    concurrency = 100
    counters = collections.Counter()

    # A timeout is set on the session, so individual requests will time out if they take too long.
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print(
            f"Starting throughput test for {test_duration}s with a concurrency of {concurrency}..."
        )
        start_time = time.monotonic()

        # Create worker tasks that will run concurrently.
        tasks = [
            asyncio.create_task(worker(session, counters)) for _ in range(concurrency)
        ]

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
    uvloop.run(main())
