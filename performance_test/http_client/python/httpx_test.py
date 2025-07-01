import time
import asyncio
import collections

import uvloop
import httpx

URL = "http://nginx:21000"


async def fetch(client: httpx.AsyncClient):
    """Performs a single HTTP GET request."""
    url = URL
    # The client timeout will handle request timeouts. For non-streaming requests,
    # httpx automatically reads the response body and releases the connection.
    await client.get(url)


async def worker(
    client: httpx.AsyncClient,
    counters: collections.Counter,
    start_event: asyncio.Event,
):
    """A worker that runs until cancelled, performing requests and counting results."""
    await start_event.wait()
    while True:
        try:
            await fetch(client)
            counters["success"] += 1
        except asyncio.CancelledError:
            # The task was cancelled, which is the signal to stop.
            break
        except Exception:
            # Any other exception is treated as a failure.
            counters["failed"] += 1


async def main():
    test_duration = 30  # seconds
    concurrency = 24
    counters = collections.Counter()
    start_event = asyncio.Event()

    # A timeout is set on the client, so individual requests will time out if they take too long.
    timeout = httpx.Timeout(10.0)
    # The default limits are 100 connections, which is what we want for this test.
    async with httpx.AsyncClient(timeout=timeout) as client:
        print(
            f"Starting throughput test for {test_duration}s with a concurrency of {concurrency}..."
        )

        # Create worker tasks. They will wait for the start_event.
        tasks = [
            asyncio.create_task(worker(client, counters, start_event))
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
