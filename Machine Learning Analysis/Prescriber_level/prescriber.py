import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv('prescriber_data_merged.csv')

df['Cost_Efficiency'] = df['total_day_supply'] / df['total_drug_cost']

cost_efficient_prescribers = df.sort_values(by='Cost_Efficiency', ascending=False)
top_cost_efficient_prescribers = cost_efficient_prescribers[['presc_id', 'presc_fullname', 'presc_state', 'Cost_Efficiency']].head(10)
print("Top 10 Cost Efficient Prescribers:")
print(top_cost_efficient_prescribers)

top_cost_efficient_prescribers.to_csv('Top_Cost_Efficient_Prescribers.csv', index=False)

state_efficiency = df.groupby('presc_state')['Cost_Efficiency'].mean().reset_index()

state_efficiency = state_efficiency.sort_values(by='Cost_Efficiency', ascending=False)

plt.figure(figsize=(10, 8))
sns.barplot(x='Cost_Efficiency', y='presc_state', data=state_efficiency, palette='viridis')
plt.xlabel('Average Cost Efficiency')
plt.ylabel('State')
plt.title('Average Cost Efficiency of Prescribers in Each State')
plt.tight_layout()

plt.savefig('state_cost_efficiency.png')

plt.show()

