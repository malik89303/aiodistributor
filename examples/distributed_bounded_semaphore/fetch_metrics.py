import asyncio

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import ujson
from redis.asyncio import Redis
from scipy import stats


async def fetch_metrics():
    redis_client = Redis(host='localhost')
    stats = await redis_client.get('accumulated_bounded_semaphore_stats')
    await redis_client.aclose()
    if stats:
        metrics = ujson.loads(stats)
        return metrics


def calculate_statistics(times):
    mean_time = np.mean(times)
    median_time = np.median(times)
    std_dev = np.std(times)
    z_scores = np.abs(stats.zscore(times))
    outliers = times[np.where(z_scores > 3)]
    return {
        'mean': mean_time,
        'median': median_time,
        'std_dev': std_dev,
        'outliers': outliers.tolist()
    }


def plot_metrics(metrics):
    server_urls = list(set(entry['server'] for entry in metrics))
    stats_summary = []

    for i, server_url in enumerate(server_urls):
        server_metrics = [entry for entry in metrics if entry['server'] == server_url]
        times = np.array([entry['ended_at'] - entry['started_at'] for entry in server_metrics])
        plt.plot(times, label=server_url)

        server_stats = calculate_statistics(times)
        stats_summary.append(
            {
                'server': server_url,
                **server_stats
            }
        )

    plt.xlabel('Request Index')
    plt.ylabel('Response Time (s)')
    plt.title('Server Response Times')
    plt.legend()
    plt.savefig('server_response_times.png')
    plt.show()

    return stats_summary


def save_statistics(stats_summary):
    df = pd.DataFrame(stats_summary)
    df.to_csv('server_statistics.csv', index=False)


if __name__ == '__main__':
    metrics = asyncio.run(fetch_metrics())
    if metrics:
        stats_summary = plot_metrics(metrics)
        save_statistics(stats_summary)
