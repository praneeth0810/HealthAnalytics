import os
import pprint as pp

#SET ENV VARIABLES
os.environ['envn'] = 'PROD'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['user'] = 'sparkuser1'
os.environ['password'] = 'user123'


#GET ENV VARIABLES
envn=os.environ['envn']
header=os.environ['header']
inferSchema=os.environ['inferSchema']
user = os.environ['user']
password = os.environ['password']

#SET OTHER VARIABLES
appName="USA Prescriber Health Analytics"
current_path = os.getcwd()
staging_dim_city = "HeallthAnalytics/staging/dimension_city"
staging_fact = "HeallthAnalytics/staging/fact"

output_city = "HeallthAnalytics/output/dimension_city"
output_fact = "HeallthAnalytics/output/presc"
