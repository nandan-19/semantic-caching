import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Set academic style for plots
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_context("paper", font_scale=1.5)

data_files = ['data/qwen.csv', 'data/gemma.csv', 'data/llama.csv']
dfs = []

for file in data_files:
    if os.path.exists(file):
        df = pd.read_csv(file)
        dfs.append(df)

if not dfs:
    print("No data files found!")
    exit()

all_data = pd.concat(dfs, ignore_index=True)

# 1. Calculate Hit Rates
print("=== OVERALL HIT RATES ===")
total_queries = len(all_data)
exact_hits = all_data['Exact_Hit'].sum()
semantic_hits = all_data['Semantic_Hit'].sum()
misses = total_queries - exact_hits - semantic_hits

print(f"Total Queries: {total_queries}")
print(f"Exact Hits (Track A): {exact_hits} ({(exact_hits/total_queries)*100:.1f}%)")
print(f"Semantic Hits (Track B): {semantic_hits} ({(semantic_hits/total_queries)*100:.1f}%)")
print(f"Misses (LLM Fallback): {misses} ({(misses/total_queries)*100:.1f}%)")

# Generate Hit Rate Chart
fig, ax = plt.subplots(figsize=(8, 6))
categories = ['Exact Hits (Track A)', 'Semantic Hits (Track B)', 'Misses (Track C)']
counts = [exact_hits, semantic_hits, misses]
colors = ['#2ca02c', '#1f77b4', '#d62728']

bars = ax.bar(categories, counts, color=colors)
ax.set_ylabel('Number of Queries')
ax.set_title('Global Cache Resolution Distribution')

for bar in bars:
    height = bar.get_height()
    ax.annotate(f'{height}\n({(height/total_queries)*100:.1f}%)',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),  
                textcoords="offset points",
                ha='center', va='bottom')

plt.tight_layout()
plt.savefig('hit_rates.png', dpi=300)
plt.close()

# 2. Latency Analysis
print("\n=== LATENCY ANALYSIS ===")
# Create categorized latency column
def categorize_latency(row):
    if row['Exact_Hit']:
        return 'Track A (Exact)'
    elif row['Semantic_Hit']:
        return 'Track B (Semantic)'
    else:
        return 'Track C (LLM Generation)'

all_data['Resolution_Path'] = all_data.apply(categorize_latency, axis=1)

latency_stats = all_data.groupby('Resolution_Path')['Total_Latency_Ms'].mean()
print(latency_stats)

plt.figure(figsize=(9, 6))
cat_order = ['Track A (Exact)', 'Track B (Semantic)', 'Track C (LLM Generation)']
means = [latency_stats.get('Track A (Exact)', 0), latency_stats.get('Track B (Semantic)', 0), latency_stats.get('Track C (LLM Generation)', 0)]
bar_colors = ['#2ca02c', '#1f77b4', '#d62728']

bars = plt.bar(cat_order, means, color=bar_colors, edgecolor='black')
plt.ylabel('Average Total Latency (ms)')
plt.title('Average Query Latency by Resolution Path')

for bar, mean_val in zip(bars, means):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 100, f"{mean_val:.1f} ms", ha='center', va='bottom', fontweight='bold', fontsize=12)

plt.ylim(0, max(means) * 1.15)
plt.tight_layout()
plt.savefig('latency_dist.png', dpi=300)
plt.close()

# 3. Model AST vs Shannon AST Agreement
print("\n=== AST AGREEMENT (Shannon vs Model) ===")
# We evaluate how often the LLM (Model AST) agreed with the deterministic Shannon AST
all_data['AST_Agreement'] = all_data['Shannon_Thresh'] == all_data['Model_Thresh']
agreement_by_model = all_data.groupby('Model')['AST_Agreement'].mean() * 100
disagreement_by_model = 100 - agreement_by_model
print(agreement_by_model)

fig, ax = plt.subplots(figsize=(9, 6))
models = agreement_by_model.index

p1 = ax.bar(models, agreement_by_model, color='#1f77b4', edgecolor='black', label='Agreed with Shannon')
p2 = ax.bar(models, disagreement_by_model, bottom=agreement_by_model, color='#d62728', edgecolor='black', label='Disagreed (Hallucination/Variance)')

ax.set_ylabel('Percentage (%)')
ax.set_title('LLM-Driven Model AST vs Deterministic Shannon Entropy')
ax.set_ylim(0, 115)
ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.0), ncol=2)

# Add text labels
for i, (agree, disagree) in enumerate(zip(agreement_by_model, disagreement_by_model)):
    ax.text(i, agree / 2, f"{agree:.1f}%", ha='center', va='center', color='white', fontweight='bold', fontsize=12)
    ax.text(i, agree + (disagree / 2), f"{disagree:.1f}%\nFailed", ha='center', va='center', color='white', fontweight='bold', fontsize=12)

plt.tight_layout()
plt.savefig('ast_agreement.png', dpi=300)
plt.close()

# 4. Semantic Trap Analysis
print("\n=== SEMANTIC TRAP ANALYSIS ===")
try:
    dataset_df = pd.read_csv('dataset.csv')
    dataset_df.rename(columns={'query': 'Query'}, inplace=True)
    merged_data = pd.merge(all_data, dataset_df, on='Query', how='left')
    
    trap_data = merged_data[merged_data['category'] == 'trap']
    total_traps = len(trap_data)
    
    if total_traps > 0:
        trap_misses = trap_data[~trap_data['Semantic_Hit'] & ~trap_data['Exact_Hit']].shape[0]
        trap_false_positives = trap_data[trap_data['Semantic_Hit']].shape[0]
        
        print(f"Total Trap Queries Processed: {total_traps}")
        print(f"Traps Correctly Rejected (Track C Miss): {trap_misses} ({(trap_misses/total_traps)*100:.1f}%)")
        print(f"Traps Incorrectly Accepted (Track B False Positives): {trap_false_positives} ({(trap_false_positives/total_traps)*100:.1f}%)")
        
        plt.figure(figsize=(6, 6))
        labels = ['Correctly Rejected\n(Routed to Track C)', 'False Positives\n(Incorrect Semantic Hit)']
        sizes = [trap_misses, trap_false_positives]
        pie_colors = ['#2ca02c', '#d62728']
        plt.pie(sizes, labels=labels, colors=pie_colors, autopct='%1.1f%%', startangle=90, wedgeprops={'edgecolor': 'black'})
        plt.title('Semantic Trap Rejection Precision')
        plt.tight_layout()
        plt.savefig('trap_rejection.png', dpi=300)
        plt.close()
        print("Saved charts to: hit_rates.png, latency_dist.png, ast_agreement.png, trap_rejection.png")
    else:
        print("No trap data found.")
except Exception as e:
    print(f"Error analyzing traps: {e}")
