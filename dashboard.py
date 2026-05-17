import json

from confluent_kafka import Consumer, KafkaError
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

KAFKA_TOPIC = "cache-telemetry"
KAFKA_SERVER = "127.0.0.1:9092"

stats = {
    "total_queries": 0,
    "traditional_hits": 0,
    "semantic_hits": 0,
    "total_misses": 0,
    "time_saved_ms": 0,
}
history = []


def create_dashboard() -> Layout:
    table = Table(
        title="🔴 Live Telemetry: Semantic vs Traditional Performance", expand=True
    )
    table.add_column("Timestamp", justify="left", style="dim", width=10)
    table.add_column("Query", style="cyan", no_wrap=True)
    table.add_column("Exact (Trad)", justify="center", width=12)
    table.add_column("Vector (Sem)", justify="center", width=12)
    table.add_column("Distance", justify="right", width=10)
    table.add_column("Trad Latency", justify="right", style="green", width=14)
    table.add_column("Sem Latency", justify="right", style="magenta", width=14)

    for event in history[-15:]:
        exact_ui = (
            "[bold green]HIT[/bold green]"
            if event["exact_match_hit"]
            else "[dim red]MISS[/dim red]"
        )
        sem_ui = (
            "[bold green]HIT[/bold green]"
            if event["semantic_match_hit"]
            else "[dim red]MISS[/dim red]"
        )
        dist_str = "-" if event["exact_match_hit"] else f"{event['distance_score']:.4f}"
        time_str = event["timestamp"].split("T")[1][:8]

        trad_lat = f"{float(event.get('trad_latency_ms', 0)):.4f}ms"
        sem_lat = f"{float(event.get('sem_latency_ms', 0)):.4f}ms"

        table.add_row(
            time_str, event["query"], exact_ui, sem_ui, dist_str, trad_lat, sem_lat
        )

    trad_rate = (
        (stats["traditional_hits"] / stats["total_queries"] * 100)
        if stats["total_queries"] > 0
        else 0
    )
    sem_rate = (
        (stats["semantic_hits"] / stats["total_queries"] * 100)
        if stats["total_queries"] > 0
        else 0
    )

    metrics_text = (
        f"Total Queries Processed:  [bold white]{stats['total_queries']}[/bold white]\n\n"
        f"Traditional Hit Rate:     [bold red]{trad_rate:.1f}%[/bold red]  ({stats['traditional_hits']} hits)\n"
        f"Semantic Hit Rate:        [bold green]{sem_rate:.1f}%[/bold green]  ({stats['semantic_hits']} hits)\n\n"
        f"Estimated Latency Saved:  [bold yellow]{stats['time_saved_ms'] / 1000:.2f} seconds[/bold yellow]"
    )

    layout = Layout()
    layout.split_column(
        Layout(
            Panel(Align.center(metrics_text), title="Thesis Metrics (Accumulative)"),
            size=10,
        ),
        Layout(table),
    )
    return layout


def main():
    console = Console()
    console.clear()

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_SERVER,
            "group.id": "fscgrpc-dashboard-viewer",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    with Live(create_dashboard(), console=console, refresh_per_second=4) as live:
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    break

                payload = json.loads(msg.value().decode("utf-8"))
                history.append(payload)

                stats["total_queries"] += 1
                if payload["exact_match_hit"]:
                    stats["traditional_hits"] += 1
                    stats["semantic_hits"] += 1
                elif payload["semantic_match_hit"]:
                    stats["semantic_hits"] += 1
                    # Base saving calculations on the semantic lookup speed
                    stats["time_saved_ms"] += 1500 - payload.get("sem_latency_ms", 15)
                else:
                    stats["total_misses"] += 1

                live.update(create_dashboard())

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
