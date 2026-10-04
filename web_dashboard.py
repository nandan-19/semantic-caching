import json
import threading

from confluent_kafka import Consumer
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

history = []
stats = {
    "total": 0,
    "exact_hits": 0,
    "trad_misses": 0,
    "fixed_hits": 0,
    "shannon_hits": 0,
    "model_hits": 0,
    "laya_called": 0,
    "laya_approved": 0,
    "laya_rejected": 0,
    "sum_trad_ms": 0,
    "sum_sem_ms": 0,
    "sum_total_ms": 0,
    "sum_laya_ms": 0,
}


def consume_kafka():
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "web-dashboard-v5",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(["cache-telemetry"])

    while True:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            payload = json.loads(msg.value().decode("utf-8"))

            stats["total"] += 1
            stats["sum_trad_ms"] += float(payload.get("trad_latency_ms", 0))
            stats["sum_sem_ms"] += float(payload.get("sem_latency_ms", 0))
            stats["sum_total_ms"] += float(payload.get("total_latency_ms", 0))
            if payload.get("laya_called"):
                stats["laya_called"] += 1
                stats["sum_laya_ms"] += float(payload.get("laya_latency_ms", 0))
                if payload.get("laya_decision") == "approved":
                    stats["laya_approved"] += 1
                else:
                    stats["laya_rejected"] += 1

            if payload.get("exact_match_hit"):
                stats["exact_hits"] += 1
            else:
                stats["trad_misses"] += 1
                dist = float(payload.get("distance_score", 2.0))

                if dist <= 0.20:
                    stats["fixed_hits"] += 1
                if dist <= float(payload.get("shannon_thresh", 0.20)):
                    stats["shannon_hits"] += 1
                if dist <= float(payload.get("model_thresh", 0.20)):
                    stats["model_hits"] += 1

            # CAPTURE THE RESPONSE TEXT FROM REDIS / OLLAMA
            # Go intercepts send this through the standard Kafka payload
            history.insert(0, payload)
            if len(history) > 50:
                history.pop()


thread = threading.Thread(target=consume_kafka, daemon=True)
thread.start()


@app.route("/api/data")
def get_data():
    t = stats["total"] if stats["total"] > 0 else 1
    avg_latencies = {
        "trad": round(stats["sum_trad_ms"] / t, 2),
        "sem": round(stats["sum_sem_ms"] / t, 2),
        "total": round(stats["sum_total_ms"] / t, 2),
        "laya": round(stats["sum_laya_ms"] / stats["laya_called"], 2) if stats["laya_called"] else 0,
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
            <div class="grid grid-cols-4 gap-4">
                <div class="bg-gray-800 p-4 rounded-lg border border-gray-600 shadow">
                    <div class="text-xs font-semibold text-gray-400 uppercase tracking-wider">Fixed Baseline (0.20)</div>
                    <div class="flex items-end gap-2 mt-1"><div class="text-4xl font-bold text-gray-300" id="stat-fixed">0</div><div class="text-sm text-gray-500 mb-1">rescued</div></div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg border border-purple-900/50 shadow">
                    <div class="text-xs font-semibold text-purple-400 uppercase tracking-wider">Shannon Entropy AST</div>
                    <div class="flex items-end gap-2 mt-1"><div class="text-4xl font-bold text-purple-400" id="stat-shannon">0</div><div class="text-sm text-gray-500 mb-1">rescued</div></div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg border border-cyan-900/50 shadow">
                    <div class="text-xs font-semibold text-cyan-400 uppercase tracking-wider">Qwen Model AST</div>
                    <div class="flex items-end gap-2 mt-1"><div class="text-4xl font-bold text-cyan-400" id="stat-model">0</div><div class="text-sm text-gray-500 mb-1">rescued</div></div>
                </div>
                <div class="bg-gray-800 p-4 rounded-lg border border-emerald-900/50 shadow">
                    <div class="text-xs font-semibold text-emerald-400 uppercase tracking-wider">Laya Safety Gate</div>
                    <div class="flex items-end gap-2 mt-1"><div class="text-4xl font-bold text-emerald-400" id="stat-laya">0/0</div><div class="text-sm text-gray-500 mb-1">approved</div></div>
                    <div class="text-xs text-gray-500 mt-1">Avg verifier latency: <span id="avg-laya" class="text-emerald-400">0</span>ms</div>
                </div>
            </div>

            <p class="text-xs text-gray-500 italic mt-4">💡 Hint: Click any query row below to view the full response text returned by the model or cache index.</p>
            <div class="bg-gray-800 rounded-lg shadow-xl overflow-hidden border border-gray-700">
                <table class="w-full text-sm text-left">
                    <thead class="text-xs text-gray-400 uppercase bg-gray-700/50">
                        <tr>
                            <th class="px-6 py-4">Query</th>
                            <th class="px-6 py-4 text-center">Hit Status</th>
                            <th class="px-4 py-4 text-right">Distance</th>
                            <th class="px-4 py-4 text-right text-purple-400">Shannon AST</th>
                            <th class="px-4 py-4 text-right text-cyan-400">Model AST</th>
                            <th class="px-4 py-4 text-center text-emerald-400">Laya Gate</th>
                            <th class="px-6 py-4 text-right text-yellow-400">Total (ms)</th>
                        </tr>
                    </thead>
                    <tbody id="table-body" class="divide-y divide-gray-700/50">
                    </tbody>
                </table>
            </div>
        </div>

        <script>
            let activeRowId = null;

            function toggleDrawer(id) {
                const drawer = document.getElementById(`drawer-${id}`);
                if (drawer.classList.contains('hidden')) {
                    drawer.classList.remove('hidden');
                    activeRowId = id;
                } else {
                    drawer.classList.add('hidden');
                    activeRowId = null;
                }
            }

            async function fetchData() {
                try {
                    const res = await fetch('/api/data');
                    const data = await res.json();

                    document.getElementById('stat-total').innerText = data.stats.total;
                    document.getElementById('stat-exact').innerText = data.stats.exact_hits;
                    document.getElementById('stat-miss').innerText = data.stats.trad_misses;
                    document.getElementById('stat-fixed').innerText = data.stats.fixed_hits;
                    document.getElementById('stat-shannon').innerText = data.stats.shannon_hits;
                    document.getElementById('stat-model').innerText = data.stats.model_hits;
                    document.getElementById('stat-laya').innerText = `${data.stats.laya_approved}/${data.stats.laya_called}`;
                    document.getElementById('avg-laya').innerText = data.avgs.laya;
                    document.getElementById('avg-trad').innerText = data.avgs.trad;
                    document.getElementById('avg-sem').innerHTML = data.avgs.sem + '<span class="text-lg">ms</span>';
                    document.getElementById('avg-total').innerText = data.avgs.total;

                    const tbody = document.getElementById('table-body');

                    // Maintain scroll view states while polling replaces data
                    const savedActiveId = activeRowId;

                    let newBodyContent = "";

                    data.history.forEach((event, index) => {
                        let statusHtml = '<span class="px-2 py-1 bg-red-900/50 text-red-400 rounded text-xs font-bold">MISS</span>';
                        if (event.exact_match_hit) statusHtml = '<span class="px-2 py-1 bg-green-900/50 text-green-400 rounded text-xs font-bold">EXACT</span>';
                        else if (event.semantic_match_hit) statusHtml = '<span class="px-2 py-1 bg-blue-900/50 text-blue-400 rounded text-xs font-bold">VECTOR</span>';

                        let dist = event.exact_match_hit ? "-" : parseFloat(event.distance_score).toFixed(3);
                        let totalLat = parseFloat(event.total_latency_ms);

                        let latColor = "text-green-400";
                        if (totalLat > 1000) latColor = "text-red-500 font-bold";
                        else if (totalLat > 100) latColor = "text-yellow-500 font-bold";

                        let shannonHit = (!event.exact_match_hit && parseFloat(dist) <= parseFloat(event.shannon_thresh));
                        let modelHit = (!event.exact_match_hit && parseFloat(dist) <= parseFloat(event.model_thresh));

                        let shannonClass = shannonHit ? "text-green-400 font-bold bg-green-950/30 px-2 py-0.5 rounded border border-green-900/40" : "text-purple-400";
                        let modelClass = modelHit ? "text-green-400 font-bold bg-green-950/30 px-2 py-0.5 rounded border border-green-900/40" : "text-cyan-400";
                        let layaHtml = '<span class="text-gray-600">—</span>';
                        if (event.laya_called) {
                            const approved = event.laya_decision === 'approved';
                            const color = approved ? 'text-emerald-400 bg-emerald-950/30 border-emerald-900/40' : 'text-red-400 bg-red-950/30 border-red-900/40';
                            const probability = parseFloat(event.laya_probability || 0).toFixed(2);
                            layaHtml = `<span class="px-2 py-0.5 rounded border text-xs font-bold ${color}">${event.laya_decision.toUpperCase()} ${probability}</span>`;
                        }

                        // Check if this specific drawer was open prior to this poll frame
                        let drawerHiddenClass = (savedActiveId == index) ? "" : "hidden";

                        // Fallback fallback handling if response field isn't packed in kafka pipeline yet
                        let modelResponseText = event.response || "No response string transmitted by message bus.";

                        newBodyContent += `
                            <tr class="hover:bg-gray-700/40 cursor-pointer transition-colors" onclick="toggleDrawer(${index})">
                                <td class="px-6 py-3 font-medium text-gray-200 truncate max-w-xs">${event.query}</td>
                                <td class="px-6 py-3 text-center">${statusHtml}</td>
                                <td class="px-4 py-3 text-right text-gray-400">${dist}</td>
                                <td class="px-4 py-3 text-right"><span class="${shannonClass}">${parseFloat(event.shannon_thresh || 0).toFixed(2)}</span></td>
                                <td class="px-4 py-3 text-right"><span class="${modelClass}">${parseFloat(event.model_thresh || 0).toFixed(2)}</span></td>
                                <td class="px-4 py-3 text-center">${layaHtml}</td>
                                <td class="px-6 py-3 text-right ${latColor}">${totalLat.toFixed(1)}</td>
                            </tr>
                            <tr id="drawer-${index}" class="${drawerHiddenClass} bg-gray-850/50">
                                <td colspan="7" class="px-8 py-4 border-l-2 border-blue-500 bg-gray-900/40 text-gray-300">
                                    <div class="space-y-2">
                                        <div><span class="text-xs font-bold text-blue-400 uppercase">Full User Prompt:</span> <span class="text-gray-100">${event.query}</span></div>
                                        <div>
                                            <span class="text-xs font-bold text-purple-400 uppercase">Pipeline Resolution Payload:</span>
                                            <pre class="mt-1 p-3 bg-gray-950 rounded text-xs overflow-x-auto font-mono text-green-400 border border-gray-800 whitespace-pre-wrap">${modelResponseText}</pre>
                                        </div>
                                        ${event.laya_called ? `<div><span class="text-xs font-bold text-emerald-400 uppercase">Laya verification:</span> ${event.laya_decision} — probability ${parseFloat(event.laya_probability || 0).toFixed(3)} / threshold ${parseFloat(event.laya_threshold || 0).toFixed(2)} (${parseFloat(event.laya_latency_ms || 0).toFixed(1)}ms)</div>` : ''}
                                    </div>
                                </td>
                            </tr>
                        `;
                    });

                    tbody.innerHTML = newBodyContent;
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
    app.run(host="0.0.0.0", port=5000, debug=False)
