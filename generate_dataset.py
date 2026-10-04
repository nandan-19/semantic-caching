import csv
import random

# Base lists for combinatorial generation
countries = ["France", "Japan", "Brazil", "Canada", "Australia", "Egypt", "India", "Germany", "Mexico", "Italy", "Spain", "South Africa", "Argentina", "Thailand", "Vietnam"]
books = ["1984", "To Kill a Mockingbird", "The Great Gatsby", "Moby Dick", "Pride and Prejudice", "Dune", "The Hobbit", "Fahrenheit 451", "Brave New World", "The Catcher in the Rye"]
foods = ["lasagna", "sushi", "chocolate chip cookies", "beef stew", "pad thai", "tacos", "fried rice", "pancakes", "grilled cheese", "pizza", "curry", "ramen"]
animals = ["elephants", "dolphins", "penguins", "tigers", "koalas", "kangaroos", "octopuses", "owls", "sharks", "wolves"]
algorithms = ["binary search", "quick sort", "merge sort", "depth first search", "breadth first search", "Dijkstra's algorithm", "A* search", "bubble sort", "insertion sort", "heap sort"]
languages = ["Python", "JavaScript", "Java", "C++", "Golang", "Rust", "Swift", "Kotlin", "TypeScript", "Ruby"]
tools = ["Docker", "Kubernetes", "Terraform", "Ansible", "Jenkins", "GitHub Actions", "Prometheus", "Grafana", "Nginx", "Apache"]
concepts_a = ["TCP", "REST", "SQL", "Mutex", "Process", "Git Merge", "Frontend", "Array", "Class", "Interface"]
concepts_b = ["UDP", "GraphQL", "NoSQL", "Semaphore", "Thread", "Git Rebase", "Backend", "Linked List", "Struct", "Abstract Class"]
math_funcs = ["derivative", "integral", "limit", "roots"]
math_eqs = ["3x^2 + 2x", "e^x", "sin(x)", "ln(x)", "x^3 - 4x + 1", "1/x", "cos(2x)", "x^2 + 5x + 6"]

prompts = []

# 1. Conversational (approx 150)
for _ in range(50):
    prompts.append((f"What is the capital of {random.choice(countries)}?", "conversational"))
    prompts.append((f"Who wrote {random.choice(books)}?", "conversational"))
    prompts.append((f"What is the best way to cook {random.choice(foods)}?", "conversational"))
    prompts.append((f"Tell me a fun fact about {random.choice(animals)}.", "conversational"))

# 2. Technical (approx 150)
for _ in range(50):
    prompts.append((f"Write a {random.choice(algorithms)} implementation in {random.choice(languages)}.", "technical"))
    prompts.append((f"How do I configure {random.choice(tools)} for production?", "technical"))
    idx = random.randint(0, len(concepts_a)-1)
    prompts.append((f"Explain the difference between {concepts_a[idx]} and {concepts_b[idx]}.", "technical"))
    prompts.append((f"What are the best practices for handling errors in {random.choice(languages)}?", "technical"))

# 3. Math (approx 100)
for _ in range(50):
    prompts.append((f"Calculate the {random.choice(math_funcs)} of {random.choice(math_eqs)}.", "math"))
    prompts.append((f"Solve for x: {random.randint(2,10)}x + {random.randint(1,20)} = {random.randint(20,100)}.", "math"))

# 4. Traps (approx 50)
# These are identical in AST structure but completely different in meaning (semantic trap)
trap_templates = [
    "Write a script to sort an array in {}",
    "How to reverse a string in {}",
    "Connect to a PostgreSQL database using {}",
    "Read a CSV file in {}",
    "Send an HTTP GET request in {}"
]
for _ in range(12):
    for template in trap_templates:
        prompts.append((template.format(random.choice(["Python", "Golang", "Java", "C++"])), "trap"))

# Shuffle the first 450 prompts to randomize the load
random.shuffle(prompts)
prompts = prompts[:450] # Enforce exact bounds

# 5. Exact Duplicates (50)
# Pick 50 existing prompts and append them at the end to force Track A (O(1) exact hits)
duplicates = random.sample(prompts, 50)
for dup in duplicates:
    prompts.append((dup[0], "duplicate"))

# Write to dataset.csv
with open("dataset.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["query", "category"])
    for p in prompts:
        writer.writerow([p[0], p[1]])

print(f"✅ Successfully generated {len(prompts)} balanced prompts in dataset.csv")
