import json
import threading

from confluent_kafka import Consumer
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# Global state for advanced comparative telemetry
history = []
stats = {
    "total": 0,
    "exact_hits": 0,
    "trad_misses": 0,
    # AST Comparison (Out of the Traditional Misses)
    "fixed_hits": 0,  # Simulated baseline (0.20)
    "shannon_hits": 0,  # Math heuristic
    "model_hits": 0,  # Qwen classification
    # Latency Tracking for Averages
    "sum_trad_ms": 0,
    "sum_sem_ms": 0,
    "sum_total_ms": 0,
}


# --- KAFKA BACKGROUND THREAD ---
def consume_kafka():
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "web-dashboard-v4",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(["cache-telemetry"])

    while True:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            payload = json.loads(msg.value().decode("utf-8"))

            stats["total"] += 1

            # Track Latencies
            stats["sum_trad_ms"] += float(payload.get("trad_latency_ms", 0))
            stats["sum_sem_ms"] += float(payload.get("sem_latency_ms", 0))
            stats["sum_total_ms"] += float(payload.get("total_latency_ms", 0))

            if payload.get("exact_match_hit"):
                stats["exact_hits"] += 1
            else:
                stats["trad_misses"] += 1
                dist = float(payload.get("distance_score", 2.0))

                # 1. Baseline Control (Fixed 0.20 Threshold)
                if dist <= 0.20:
                    stats["fixed_hits"] += 1

                # 2. Shannon AST Hit Check
                if dist <= float(payload.get("shannon_thresh", 0.20)):
                    stats["shannon_hits"] += 1

                # 3. Model AST Hit Check
                if dist <= float(payload.get("model_thresh", 0.20)):
                    stats["model_hits"] += 1

            history.insert(0, payload)
            if len(history) > 50:
                history.pop()


thread = threading.Thread(target=consume_kafka, daemon=True)
thread.start()


# --- WEB ENDPOINTS ---
@app.route("/api/data")
def get_data():
    # Calculate Averages safely
    t = stats["total"] if stats["total"] > 0 else 1
    avg_latencies = {
        "trad": round(stats["sum_trad_ms"] / t, 2),
        "sem": round(stats["sum_sem_ms"] / t, 2),
        "total": round(stats["sum_total_ms"] / t, 2),
    }
    return jsonify({"stats": stats, "history": history, "avgs": avg_latencies})


@app.route("/")
def index():
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Semantic Cache | Thesis Benchmark</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-900 text-gray-100 font-sans p-8 min-h-screen">
        <div class="max-w-7xl mx-auto space-y-6">

            <header class="border-b border-gray-700 pb-4">
                <h1 class="text-3xl font-bold text-blue-400 flex items-center gap-3">
                    <span class="relative flex h-3 w-3"><span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-blue-500"></span></span>
                    Gateway Telemetry Dashboard
                </h1>
                <p class="text-gray-400 mt-2">Live 4-Way Architecture Benchmark Monitoring</p>
            </header>

            <div class="grid grid-cols-4 gap-4">
                <div class="bg-gray-800 p-4 rounded-lg border border-gray-700 shadow">
                    <div class="text-xs font-semibold text-gray-400 uppercase tracking-wider">Total Queries</div>
                    <div class="text-3xl font-bold mt-1" id="stat-total">0</div>
                    <div class="text-xs text-gray-500 mt-1">Avg Sys Load: <span id="avg-total" class="text-yellow-400">0</span>ms</div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg border border-green-900/50 shadow">
                    <div class="text-xs font-semibold text-gray-400 uppercase tracking-wider">Track A: Exact Hits</div>
                    <div class="text-3xl font-bold text-green-400 mt-1" id="stat-exact">0</div>
                    <div class="text-xs text-gray-500 mt-1">Avg Network Speed: <span id="avg-trad" class="text-green-400">0</span>ms</div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg border border-red-900/50 shadow">
                    <div class="text-xs font-semibold text-gray-400 uppercase tracking-wider">Track A: Misses</div>
                    <div class="text-3xl font-bold text-red-400 mt-1" id="stat-miss">0</div>
                    <div class="text-xs text-gray-500 mt-1">Passed to Semantic Layer</div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg border border-blue-900/50 shadow">
                    <div class="text-xs font-semibold text-gray-400 uppercase tracking-wider">Track B: Vector Speed</div>
                    <div class="text-3xl font-bold text-blue-400 mt-1" id="avg-sem">0<span class="text-lg">ms</span></div>
                    <div class="text-xs text-gray-500 mt-1">Avg AI Embedding Latency</div>
                </div>
            </div>

            <h2 class="text-lg font-bold text-gray-300 mt-8">Semantic Rescue Logic (Head-to-Head)</h2>
            <div class="grid grid-cols-3 gap-4">
                <div class="bg-gray-800 p-4 rounded-lg border border-gray-600 shadow relative overflow-hidden">
                    <div class="text-xs font-semibold text-gray-400 uppercase tracking-wider">Fixed Baseline (0.20)</div>
                    <div class="flex items-end gap-2 mt-1">
                        <div class="text-4xl font-bold text-gray-300" id="stat-fixed">0</div>
                        <div class="text-sm text-gray-500 mb-1">rescued</div>
                    </div>
                    <div class="text-xs text-gray-500 mt-2">Dumb threshold. High poisoning risk.</div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg border border-purple-900/50 shadow relative overflow-hidden">
                    <div class="text-xs font-semibold text-purple-400 uppercase tracking-wider">Shannon Entropy AST</div>
                    <div class="flex items-end gap-2 mt-1">
                        <div class="text-4xl font-bold text-purple-400" id="stat-shannon">0</div>
                        <div class="text-sm text-gray-500 mb-1">rescued</div>
                    </div>
                    <div class="text-xs text-gray-500 mt-2">Math-driven. Zero latency overhead.</div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg border border-cyan-900/50 shadow relative overflow-hidden">
                    <div class="text-xs font-semibold text-cyan-400 uppercase tracking-wider">Qwen Model AST</div>
                    <div class="flex items-end gap-2 mt-1">
                        <div class="text-4xl font-bold text-cyan-400" id="stat-model">0</div>
                        <div class="text-sm text-gray-500 mb-1">rescued</div>
                    </div>
                    <div class="text-xs text-gray-500 mt-2">LLM intent-driven. Heavy latency block.</div>
                </div>
            </div>

            <div class="bg-gray-800 rounded-lg shadow-xl overflow-hidden border border-gray-700 mt-6">
                <table class="w-full text-sm text-left">
                    <thead class="text-xs text-gray-400 uppercase bg-gray-700/50">
                        <tr>
                            <th class="px-6 py-4">Query</th>
                            <th class="px-6 py-4 text-center">Hit Status</th>
                            <th class="px-4 py-4 text-right">Distance</th>
                            <th class="px-4 py-4 text-right text-purple-400">Shannon AST</th>
                            <th class="px-4 py-4 text-right text-cyan-400">Model AST</th>
                            <th class="px-6 py-4 text-right text-yellow-400">Total (ms)</th>
                        </tr>
                    </thead>
                    <tbody id="table-body" class="divide-y divide-gray-700/50">
                    </tbody>
                </table>
            </div>
        </div>

        <script>
            async function fetchData() {
                try {
                    const res = await fetch('/api/data');
                    const data = await res.json();

                    // Update Core Stats
                    document.getElementById('stat-total').innerText = data.stats.total;
                    document.getElementById('stat-exact').innerText = data.stats.exact_hits;
                    document.getElementById('stat-miss').innerText = data.stats.trad_misses;

                    // Update AST Comparatives
                    document.getElementById('stat-fixed').innerText = data.stats.fixed_hits;
                    document.getElementById('stat-shannon').innerText = data.stats.shannon_hits;
                    document.getElementById('stat-model').innerText = data.stats.model_hits;

                    // Update Averages
                    document.getElementById('avg-trad').innerText = data.avgs.trad;
                    document.getElementById('avg-sem').innerHTML = data.avgs.sem + '<span class="text-lg">ms</span>';
                    document.getElementById('avg-total').innerText = data.avgs.total;

                    // Update Table
                    const tbody = document.getElementById('table-body');
                    tbody.innerHTML = '';

                    data.history.forEach(event => {
                        let statusHtml = '<span class="px-2 py-1 bg-red-900/50 text-red-400 rounded text-xs font-bold">MISS</span>';
                        if (event.exact_match_hit) statusHtml = '<span class="px-2 py-1 bg-green-900/50 text-green-400 rounded text-xs font-bold">EXACT</span>';
                        else if (event.semantic_match_hit) statusHtml = '<span class="px-2 py-1 bg-blue-900/50 text-blue-400 rounded text-xs font-bold">VECTOR</span>';

                        let dist = event.exact_match_hit ? "-" : parseFloat(event.distance_score).toFixed(3);
                        let totalLat = parseFloat(event.total_latency_ms);

                        let latColor = "text-green-400";
                        if (totalLat > 1000) latColor = "text-red-500 font-bold";
                        else if (totalLat > 100) latColor = "text-yellow-500 font-bold";

                        const row = `
                            <tr class="hover:bg-gray-700/20 transition-colors">
                                <td class="px-6 py-3 font-medium text-gray-200 truncate max-w-xs">${event.query}</td>
                                <td class="px-6 py-3 text-center">${statusHtml}</td>
                                <td class="px-4 py-3 text-right text-gray-400">${dist}</td>
                                <td class="px-4 py-3 text-right text-purple-400">${parseFloat(event.shannon_thresh || 0).toFixed(2)}</td>
                                <td class="px-4 py-3 text-right text-cyan-400">${parseFloat(event.model_thresh || 0).toFixed(2)}</td>
                                <td class="px-6 py-3 text-right ${latColor}">${totalLat.toFixed(1)}</td>
                            </tr>
                        `;
                        tbody.innerHTML += row;
                    });
                } catch (err) {}
            }
            setInterval(fetchData, 1000);
            fetchData();
        </script>
    </body>
    </html>
    """
    return render_template_string(html)


if __name__ == "__main__":
    import logging

    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=5000, debug=False)
