"""simple example on how to use uuuu."""
import random
from uuuu import iterables


def generate_logs():
    """Simulate a stream of log messages (with different log levels)"""
    levels = ['INFO', 'ERROR', 'WARNING']
    for _ in range(100):
        yield {
            'level': random.choice(levels),
            'message': f"Log message {_}"
        }


def process_logs():
    """Process log stream using your library"""
    # Generate a stream of logs
    logs = generate_logs()

    # Step 1: Split the logs into errors and non-errors
    error_logs, _ = iterables.split(lambda log: log['level'] == 'ERROR', logs)

    # Step 2: Batch the error logs in groups of 5
    error_batches = iterables.batches(error_logs, batch_size=5)

    # Step 3: Apply rolling aggregation to count the cumulative number of error logs
    rolling_error_counts = iterables.rolling_aggregate(
        error_batches, lambda acc, batch: acc + len(batch), initial=0)

    # Step 4: Throttle the output to 1 batch per second to simulate output rate control
    throttled_output = iterables.throttle(rolling_error_counts, max_rate=1)

    # Step 5: Exhaust the iterator to consume the logs and print each batch count
    iterables.exhaust(print(batch) for batch in throttled_output)


if __name__ == "__main__":
    process_logs()
