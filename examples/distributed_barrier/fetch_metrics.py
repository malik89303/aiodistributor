import asyncio

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import ujson
from redis.asyncio import Redis
from scipy import stats


async def fetch_metrics():
    redis_client = Redis(host='localhost')
    stats = await redis_client.get('accumulated_barrier_stats')
    await redis_client.aclose()
    if stats:
        metrics = ujson.loads(stats)
        return metrics
    else:
        print('No data found in Redis.')
        return []


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
    if not metrics:
        print('No metrics to plot.')
        return

    server_urls = list(set(entry['server'] for entry in metrics))
    stats_summary = []

    plt.figure(figsize=(14, 8))

    for server_url in server_urls:
        server_metrics = [entry for entry in metrics if entry['server'] == server_url]
        if not server_metrics:
            continue

        times = np.array([entry['ended_at'] - entry['started_at'] for entry in server_metrics])
        if times.size == 0:
            continue

        plt.plot(times, label=server_url)

        server_stats = calculate_statistics(times)
        stats_summary.append(
            {
                'server': server_url,
                **server_stats
            }
        )

    if stats_summary:
        plt.xlabel('Request Index')
        plt.ylabel('Response Time (s)')
        plt.title('Server Response Times')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout(rect=[0, 0, 0.9, 1])
        plt.savefig('server_response_times.png', bbox_inches='tight')
        plt.savefig('server_response_times.png')
        plt.show()
    else:
        print('No data to plot.')

    return stats_summary


def save_statistics(stats_summary):
    if stats_summary:
        df = pd.DataFrame(stats_summary)
        df.to_csv('server_statistics.csv', index=False)
    else:
        print('No statistics to save.')


if __name__ == '__main__':
    metrics = asyncio.run(fetch_metrics())
    if metrics:
        stats_summary = plot_metrics(metrics)
        save_statistics(stats_summary)
    else:
        print('No metrics retrieved.')
