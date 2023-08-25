import requests




AIRFLOW_API_BASE_URL = 'http://localhost:8080/api/v1/dags/dag-sleep_2'

AIRFLOW_USERNAME = 'Admin'

AIRFLOW_PASSWORD = '7A6WrqcdpK8bA66A'




def fetch_all_dag_run():

    url = f'{AIRFLOW_API_BASE_URL}/dagRuns'

    response = requests.get(url, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))

    if response.status_code == 200:

        dagrun = response.json()

        return dagrun

    else:

        print(f"Error retrieving DAGs. Status code: {response.status_code}")

        return None




# Example usage

all_dagrun = fetch_all_dag_run()




if all_dagrun:

    print(all_dagrun)