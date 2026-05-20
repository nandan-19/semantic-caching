import subprocess
import time

prompts = [
    # --- CONVERSATIONAL (Low Entropy -> Loose Threshold, Semantic Hits Expected) ---
    "How do I play chess?",
    "What are the rules of chess?",
    "Explain chess to a beginner.",
    "Who won the last football world cup?",
    "Tell me about the football world cup.",
    "What is the capital of France?",
    "Tell me the capital of France.",
    "How do you properly boil an egg?",
    "What's the best way to boil eggs?",
    "Who wrote the play Romeo and Juliet?",
    "Give me a summary of Romeo and Juliet.",
    # --- HIGH-PRECISION / CODE (High Entropy -> Strict Threshold) ---
    "Write a quick swap function in C++",
    "How to write a quick swap function in C?",
    "Implement variable swap in golang",
    "gRPC unary interceptor golang example",
    "How to build an interceptor in gRPC Go",
    "How to implement a singleton pattern in Java?",
    "Java singleton design pattern example",
    "What is the difference between a mutex and a semaphore?",
    "Mutex vs semaphore in operating systems",
    "Docker compose volume mapping syntax",
    "How to mount a volume in docker-compose.yml",
    "Explain B-tree vs Hash index in databases",
    # --- SEMANTIC TRAPS (Proves why AST is mandatory!) ---
    "Write a python script to sort an array",
    "Write a golang script to sort an array",
    "Show me a recursive Fibonacci function in C",
    "Iterative Fibonacci sequence in C",
    "How to reverse a string in JavaScript",
    "JavaScript string reversal method",
    # --- MATH / FORMULAIC (High Entropy -> Strict Threshold) ---
    "Calculate derivative of 4x^3 + 2x",
    "Find the integral of x squared",
    "O(n log n) sorting algorithm python",
    "Quick sort complexity analysis",
    "What is the Pythagorean theorem?",
    "Formula for the area of a circle",
    "Calculate the eigenvalues of a 2x2 identity matrix",
    "How to solve a quadratic equation",
    # --- EXACT DUPLICATES (Forces Track A to fire O(1) Hits) ---
    "How do I play chess?",
    "Write a quick swap function in C++",
    "gRPC unary interceptor golang example",
    "What is the difference between a mutex and a semaphore?",
    "How do you properly boil an egg?",
    "Formula for the area of a circle",
    "Calculate derivative of 4x^3 + 2x",
    "Explain chess to a beginner.",
    "Tell me about the football world cup.",
    "Docker compose volume mapping syntax",
    "How to mount a volume in docker-compose.yml",
    "How to implement a singleton pattern in Java?",
    "Calculate the eigenvalues of a 2x2 identity matrix",
    "Write a python script to sort an array",
]

print(f"🚀 Launching Benchmark Matrix: {len(prompts)} queries...\n")

for i, query in enumerate(prompts):
    print(f"[{i + 1}/{len(prompts)}] Sending: '{query[:40]}...'")

    cmd = f"""grpcurl -plaintext -import-path ../proto -proto api.proto -d '{{"text": "{query}"}}' localhost:50051 semantic_cache.CoreAppService/ProcessQuery"""

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"  ❌ execution failure: {result.stderr.strip()}")

    time.sleep(2.0)

print("\n✅ Benchmark Complete. Check Dashboard.")
