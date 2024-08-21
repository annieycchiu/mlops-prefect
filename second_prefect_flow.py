import httpx
from prefect import flow


@flow(log_prints=True)
def get_repo_info(repo_name: str = "PrefectHQ/prefect"):
    url = f"https://api.github.com/repos/{repo_name}"
    response = httpx.get(url)
    response.raise_for_status()
    repo = response.json()
    print(f"{repo_name} repository statistics 🤓:")
    print(f"Stars 🌠 : {repo['stargazers_count']}")
    print(f"Forks 🍴 : {repo['forks_count']}")


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/annieycchiu/mlops-prefect.git",
        entrypoint="second_prefect_flow.py:get_repo_info",
    ).deploy(
        name="my-prefect-deployment-to-pool", 
        work_pool_name="my-managed-pool", 
    )

# import os
# import json
# from prefect import task, flow
# from prefect.client.schemas.schedules import IntervalSchedule, CronSchedule
# from datetime import datetime, timedelta
# import pandas as pd

# @task(cache_expiration=timedelta(hours=1))
# def fetch_data(wrong_data=123):
#     data = [
#         {'name': 'Andy', 'age': '33', 'time': datetime.now(), 'random': wrong_data},
#         {'name': 'Bob', 'age': '10', 'time': datetime.now(), 'wrong': wrong_data},
#         ]
    
#     return data

# @task
# def transform_data(data):
#     needed_keys = ['name', 'age', 'time']

#     filtered_data = []
#     for d in data:
#         filtered_dict = {} 
#         for k, v in d.items():
#             if k in needed_keys: 
#                 if v is not None and not isinstance(v, str):
#                     v = str(v)
#                 filtered_dict[k] = v 
#         filtered_data.append(filtered_dict) 
#     return filtered_data

# @task
# def save_data(data):
#     data = pd.DataFrame(data)

#     # Path to your CSV file
#     csv_file = 'the_data.csv'

#     # Append new data to the CSV file
#     data.to_csv(csv_file, mode='a', index=False, header=False)


# # Define the Prefect flow
# @flow(name="ETL Flow - v2", log_prints=True)
# def etl_flow_2(wrong_data=10):
#     data = fetch_data(wrong_data)
#     transformed_data = transform_data(data)
#     save_data(transformed_data)

#     print('complete the task!')

# # Run the flow
# if __name__ == "__main__":
#     etl_flow_2()#.serve(name="my-first-deployment")