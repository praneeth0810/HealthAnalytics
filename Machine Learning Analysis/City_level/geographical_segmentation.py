import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
import json

def load_data(filepath):
    data = []
    with open(filepath, 'r') as file:
        for line in file:
            json_obj = json.loads(line)
            data.append(json_obj)
    return pd.DataFrame(data)

def scale_features(data):
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(data[['trx_counts', 'presc_counts', 'population']])
    return scaled_data

def perform_clustering(data, scaled_data):
    kmeans = KMeans(n_clusters=3, random_state=0).fit(scaled_data)
    data['cluster'] = kmeans.labels_
    return data, kmeans

def plot_clusters(data):
    plt.figure(figsize=(10, 6))
    plt.scatter(data['population'], data['presc_counts'], c=data['cluster'], cmap='viridis', alpha=0.5)
    cluster_2_data = data[data['cluster'] == 2]
    plt.scatter(cluster_2_data['population'], cluster_2_data['presc_counts'], c='red', label='Cluster 2', alpha=1, edgecolor='black')
    plt.xlabel('Population')
    plt.ylabel('Prescription Counts')
    plt.title('Geographical Segmentation by Population and Prescription Counts')
    plt.colorbar(label='Cluster')
    plt.legend()
    plt.show()

    
def extract_cluster_data(data, cluster_label):
    return data[data['cluster'] == cluster_label]

def main():
    data = load_data('prescriber1.json') 
    scaled_data = scale_features(data)
    data, kmeans = perform_clustering(data, scaled_data)
    plot_clusters(data)
    cluster_data = extract_cluster_data(data, 2) 
    print("Data from Cluster 2:")
    print(cluster_data.head())
    data.to_csv('clustered_data.csv', index=False)
    cluster_data.to_csv('cluster_2_data.csv', index=False)


if __name__ == "__main__":
    main()
