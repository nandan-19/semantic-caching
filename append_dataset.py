import csv

new_prompts = [
    # 1. Mars
    ("Does anyone know if there is water on Mars?", "conversational"),
    ("Does anyone know if there is water on Mars?", "duplicate"),
    ("Has water ever been discovered on the planet Mars?", "conversational"),
    ("Does anyone know if there is water on Venus?", "trap"),
    
    # 2. Docker
    ("I'm totally new, how do I start a Docker container?", "technical"),
    ("I'm totally new, how do I start a Docker container?", "duplicate"),
    ("What's the command to run a new container in Docker?", "technical"),
    ("I'm totally new, how do I stop a Docker container?", "trap"),
    
    # 3. Kubernetes
    ("Pls help, what exactly is a Kubernetes pod?", "technical"),
    ("Pls help, what exactly is a Kubernetes pod?", "duplicate"),
    ("Can someone explain the concept of a Pod in K8s?", "technical"),
    ("Pls help, what exactly is a Kubernetes node?", "trap"),
    
    # 4. Integral of x^2
    ("Math homework: what is the integral of x squared?", "math"),
    ("Math homework: what is the integral of x squared?", "duplicate"),
    ("Can you integrate x^2 with respect to x for me?", "math"),
    ("Math homework: what is the derivative of x squared?", "trap"),
    
    # 5. Matrix Transpose
    ("How do I find the transpose of a 3x3 matrix?", "math"),
    ("How do I find the transpose of a 3x3 matrix?", "duplicate"),
    ("What are the steps to transpose a 3 by 3 matrix?", "math"),
    ("How do I find the inverse of a 3x3 matrix?", "trap"),
    
    # 6. Capital of Canada
    ("Quick geography question, what's the capital of Canada?", "conversational"),
    ("Quick geography question, what's the capital of Canada?", "duplicate"),
    ("What city is the official capital of Canada?", "conversational"),
    ("Quick geography question, what's the capital of Mexico?", "trap"),
    
    # 7. Capital of Japan
    ("I'm visiting soon, what is the capital of Japan?", "conversational"),
    ("I'm visiting soon, what is the capital of Japan?", "duplicate"),
    ("Name the capital city of Japan for me.", "conversational"),
    ("I'm visiting soon, what is the capital of China?", "trap"),
    
    # 8. WWII
    ("History test tomorrow! What year did World War 2 end?", "conversational"),
    ("History test tomorrow! What year did World War 2 end?", "duplicate"),
    ("In what year did the Second World War officially finish?", "conversational"),
    ("History test tomorrow! What year did World War 1 end?", "trap"),
    
    # 9. Moon Landing
    ("When exactly did humans first land on the moon?", "conversational"),
    ("When exactly did humans first land on the moon?", "duplicate"),
    ("What date did the Apollo 11 moon landing happen?", "conversational"),
    ("When exactly did humans first land on Mars?", "trap"),
    
    # 10. Scrambled eggs
    ("I'm hungry, what's the secret to fluffy scrambled eggs?", "conversational"),
    ("I'm hungry, what's the secret to fluffy scrambled eggs?", "duplicate"),
    ("How do you make the best, fluffiest scrambled eggs?", "conversational"),
    ("I'm hungry, what's the secret to fluffy boiled eggs?", "trap"),
    
    # 11. DNA
    ("Biology question: what does DNA stand for?", "conversational"),
    ("Biology question: what does DNA stand for?", "duplicate"),
    ("What is the full scientific name for DNA?", "conversational"),
    ("Biology question: what does RNA stand for?", "trap"),
    
    # 12. Periodic Table
    ("What is the very first element on the periodic table?", "conversational"),
    ("What is the very first element on the periodic table?", "duplicate"),
    ("Name element number 1 on the periodic table.", "conversational"),
    ("What is the very last element on the periodic table?", "trap"),
    
    # 13. Blockchain
    ("I keep hearing about it, what actually is a blockchain?", "technical"),
    ("I keep hearing about it, what actually is a blockchain?", "duplicate"),
    ("Explain the concept of blockchain technology simply.", "technical"),
    ("I keep hearing about it, what actually is a cryptocurrency?", "trap"),
    
    # 14. Van Gogh
    ("Art history: why did Vincent Van Gogh cut off his ear?", "conversational"),
    ("Art history: why did Vincent Van Gogh cut off his ear?", "duplicate"),
    ("What's the real story behind Van Gogh losing his ear?", "conversational"),
    ("Art history: why did Vincent Van Gogh paint sunflowers?", "trap"),
    
    # 15. Olympics
    ("How often are the Summer Olympic games held?", "conversational"),
    ("How often are the Summer Olympic games held?", "duplicate"),
    ("What is the time gap between each Summer Olympics?", "conversational"),
    ("How often are the Winter Olympic games held?", "trap")
]

with open("dataset.csv", mode='a', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    for p in new_prompts:
        writer.writerow([p[0], p[1]])

print("Successfully appended 60 prompts!")
