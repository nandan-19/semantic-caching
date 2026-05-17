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
}
history = []


def create_dashboard() -> Layout:
    table = Table(title="🔴 Live Telemetry: End-to-End Latency Tracking", expand=True)
    table.add_column("Time", justify="left", style="dim", width=10)
    table.add_column("Query", style="cyan", no_wrap=True)
    table.add_column("Trad Hit", justify="center", width=10)
    table.add_column("Sem Hit", justify="center", width=10)
    table.add_column("Dist", justify="right", width=8)
    table.add_column("Trad Lat", justify="right", style="green", width=10)
    table.add_column("Sem Lat", justify="right", style="magenta", width=10)
    table.add_column("Total Lat (User)", justify="right", style="yellow", width=16)

    for event in history[-12:]:
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
        dist_str = "-" if event["exact_match_hit"] else f"{event['distance_score']:.3f}"
        time_str = event["timestamp"].split("T")[1][:8]

        trad_lat = f"{float(event.get('trad_latency_ms', 0.0)):.2f}ms"
        sem_lat = f"{float(event.get('sem_latency_ms', 0.0)):.2f}ms"

        # Color code the total latency
        total_float = float(event.get("total_latency_ms", 0.0))
        if total_float > 1000:
            total_lat_ui = f"[bold red]{total_float:.2f}ms[/bold red]"  # Cloud penalty
        elif total_float > 100:
            total_lat_ui = (
                f"[bold yellow]{total_float:.2f}ms[/bold yellow]"  # Local SLM penalty
            )
        else:
            total_lat_ui = (
                f"[bold green]{total_float:.2f}ms[/bold green]"  # Cache Hit Speed
            )

        table.add_row(
            time_str,
            event["query"],
            exact_ui,
            sem_ui,
            dist_str,
            trad_lat,
            sem_lat,
            total_lat_ui,
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
        f"Traditional Hit Rate:     [bold red]{trad_rate:.1f}%[/bold red]\n"
        f"Semantic Hit Rate:        [bold green]{sem_rate:.1f}%[/bold green]\n"
    )

    layout = Layout()
    layout.split_column(
        Layout(
            Panel(Align.center(metrics_text), title="Thesis Metrics (Accumulative)"),
            size=8,
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
            "group.id": "fscgrpc-dashboard-viewer-v2",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    with Live(create_dashboard(), console=console, refresh_per_second=4) as live:
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue

                payload = json.loads(msg.value().decode("utf-8"))
                history.append(payload)

                stats["total_queries"] += 1
                if payload["exact_match_hit"]:
                    stats["traditional_hits"] += 1
                    stats["semantic_hits"] += 1
                elif payload["semantic_match_hit"]:
                    stats["semantic_hits"] += 1
                else:
                    stats["total_misses"] += 1

                live.update(create_dashboard())
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
