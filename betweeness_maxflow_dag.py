from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
local_tz = timezone('Asia/Kolkata')


import psycopg2
import networkx as nx
import networkx.algorithms.community as nx_comm
import pandas as pd
import numpy as np
import psycopg2
import operator
from sqlalchemy import create_engine
from pandasql import sqldf

def main(network_type,engine,calculation_type):

    try:
    
        if calculation_type=="isis":

            print("Inside the isis :",calculation_type)

            query = f"SELECT lower(exit_route) as exit_route, lower(from_node) as from_node, lower(to_node) as to_node, from_isis as metric_value,network_type, lower(from_interface) as from_interface, lower(to_interface) as to_interface,'isis' as calculation_type FROM bbip_route_details a left join cacti_usage_interfaces b on a.from_node=b.inthost where network_type = '{network_type}' and from_isis > '0'"

        elif calculation_type =="capacity":

            print("Inside the capacity :",calculation_type)
            query = f"SELECT lower(exit_route) as exit_route, lower(from_node) as from_node, lower(to_node) as to_node, b.cacti_bw as metric_value, network_type, lower(from_interface) as from_interface, lower(to_interface) as to_interface,'capacity' as calculation_type FROM bbip_route_details a left join cacti_usage_interfaces b on a.from_node=b.inthost where network_type = '{network_type}' and cacti_bw > '0'"
        else:
            print("someting went wrong")


        routers = pd.read_sql(query, engine)

        bbip_route_details_df = pd.read_sql(query, engine)

        routers = routers[['from_node', 'to_node', 'metric_value','exit_route']]

        routers['from_node'] = routers['from_node'].astype(str)
        routers['to_node'] = routers['to_node'].astype(str)
        routers['metric_value'] = routers['metric_value'].astype(float)

        routers['from_node'] = routers['from_node'].apply(lambda x: x.lower())
        routers['to_node'] = routers['to_node'].apply(lambda x: x.lower())

        routers['exit_route'] = routers['exit_route'].apply(lambda x: x.lower())

        routers['link_name'] = routers.apply(lambda row: '_'.join(sorted([row['from_node'], row['to_node']])), axis=1)

        #routers.drop_duplicates(subset=['link_name'], inplace=True) # may 23

        #routers.head()

        routers['bw'] = routers.apply(lambda row: row['metric_value'], axis=1)
        routers['bw'] = pd.to_numeric(routers['bw'], errors='coerce')

        #routers['bw_inv'] = routers['bw'].apply(lambda x: 1/x)
        #routers

        s1 = routers['from_node']
        s2 = routers['to_node']
        ss = routers['exit_route']
        s3 = pd.concat([s1, s2, ss])

        #s3 = s3.unique() #may 23 


        from sklearn.preprocessing import MinMaxScaler, StandardScaler
        scaler = MinMaxScaler()
        routers['bw_norm'] = scaler.fit_transform(routers['bw'].values.reshape(-1, 1))
        routers['bw_norm_inv'] = routers['bw_norm'].apply(lambda x: 1 - x)
        #routers['bw_norm_inv'] = routers['bw_norm'].apply(lambda x: -1*x)

        routers_bbip = routers[routers['from_node'].isin(s3) & routers['to_node'].isin(s3)]

        J = nx.MultiGraph()
        #J = nx.Graph()

        for node in s3:
            J.add_node(node)

        for _, edge in routers_bbip[['from_node', 'to_node', 'bw_norm_inv', 'bw_norm', 'metric_value', 'exit_route']].iterrows():
            #J.add_edge(edge['from_node'],edge['to_node'], weight=edge['bw_norm_inv'], metric=edge['from_isis'])
            J.add_edge(edge['from_node'], edge['to_node'], key=edge['exit_route'], weight=edge['bw_norm_inv'], metric=edge['metric_value'])

        idx_small_c = [i for i, g in enumerate(list(nx.connected_components(J))) if len(g) < 10]

        el_small_c = []
        for i in idx_small_c:
            g = list(list(nx.connected_components(J))[i])
            el_small_c.extend(g)

        for el in el_small_c:
            J.remove_node(el)

        sources = [n for n in J.nodes() if 'hl1' in n]
        targets = [n for n in J.nodes() if 'hl1' not in n]
        #btw_subset = nx.edge_betweenness_centrality_subset(J, targets=targets, sources=sources,weight='metric', normalized=False)

        btw_subset = nx.edge_betweenness_centrality_subset(J, targets=targets, sources=sources,weight='metric', normalized=False)

        #btw_subset = nx.betweenness_centrality_subset(J, targets=targets, sources=sources, weight='metric', normalized=True)
        btw_subset = {k: v for k, v in sorted(btw_subset.items(), key=lambda item: item[1], reverse=True)}
        #btw_subset

        route_impact_analysis_betweeness_df = pd.DataFrame(btw_subset.items(), columns=['Key', 'Value'])
        route_impact_analysis_betweeness_df[['from_node', 'to_node', 'exit_route']] = pd.DataFrame(route_impact_analysis_betweeness_df['Key'].tolist())
        route_impact_analysis_betweeness_df.drop('Key', axis=1, inplace=True)

        route_impact_analysis_betweeness_df = route_impact_analysis_betweeness_df.rename(columns={'Value': 'betweenness_prior_nw_impact'})

        
        route_impact_analysis_betweeness_df[['source_router_host', 'from_interface', 'target_router_host', 'to_interface', 'OUTBOUND', 'from_state', 'from_city']] = route_impact_analysis_betweeness_df['exit_route'].str.split('|', expand=True)
        route_impact_analysis_betweeness_df = route_impact_analysis_betweeness_df.drop(['exit_route', 'source_router_host', 'target_router_host', 'OUTBOUND', 'from_state', 'from_city'], axis=1)

        final_networktype_internet = "select distinct a.from_node, a.to_node, a.from_interface,a.to_interface, a.betweenness_prior_nw_impact, b.network_type,b.metric_value,b.calculation_type from route_impact_analysis_betweeness_df a left join bbip_route_details_df b on a.from_node = b.from_node and a.to_node = b.to_node and a.from_interface = b.from_interface and a.to_interface = b.to_interface"

        final_networktype_internet = sqldf(final_networktype_internet)

        return final_networktype_internet

    except Exception as e:
            # Handle the exception
            print("Something Went Wrong :", str(e))
            # Rollback the changes
            #connection.rollback()

def store(df,engine,cursor,connection):

    try:
        # Execute the TRUNCATE statement
        # cursor.execute("TRUNCATE TABLE route_impact_analysis_betweeness")
        # # Commit the changes
        # connection.commit()

        print("Table truncated successfully.")

        df.to_sql('route_impact_analysis_betweeness',engine,if_exists='append',index=False)

        print("Table Inserted Successfully")

        #print(df)     

    except Exception as e:
        # Handle the exception
        print("Error occurred while truncating the table:", str(e))
        # Rollback the changes
        connection.rollback()

       # Close the cursor and connection

def find_betweeness():

    params = {
        'host': "localhost",
        'port': "5432",
        'database': "uc12",
        'user': "postgres",
        'password': "131120"
    }


    # params = {
    #     'host': "telco-postgresql-mvp-primary.telco-datastorage-mvp.svc.cluster.local",
    #     'port': "5432",
    #     'database': "heatmap",
    #     'user': "postgres",
    #     'password':"admintelco100"
    # }

    connection_string = f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"

    engine = create_engine(connection_string)

    connection = psycopg2.connect(
        host=params['host'],
        port=params['port'],
        database=params['database'],
        user=params['user'],
        password=params['password']
    )

    cursor = connection.cursor()

    network_type = "internet backbone"  # Set the network type here
    calculation_type = "isis"
    internet_isis_df = main(network_type,engine,calculation_type)
    #print("isis :",internet_isis_df)

    network_type = "internet backbone"  # Set the network type here
    calculation_type = "capacity"
    internet_metric_df = main(network_type,engine,calculation_type)
    #print("capacity :",internet_metric_df)

    network_type = "critical backbone"  # Set the network type here
    calculation_type = "isis"
    critical_isis_df = main(network_type,engine,calculation_type)
    #print("isis :",critical_isis_df)

    network_type = "critical backbone"  # Set the network type here
    calculation_type = "capacity"
    critical_metric_df = main(network_type,engine,calculation_type)
    #print("capacity :",critical_metric_df)


    df = pd.concat([internet_isis_df,internet_metric_df,critical_isis_df,critical_metric_df])

    #print(df)

    store(df,engine,cursor,connection)

    engine.dispose() 
    cursor.close()
    connection.close()
    print("close connection")





default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,6,13)),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'catchup': False,

}

with DAG(
    dag_id='betweeness_maxflow',
    default_args=default_args,
    description="Checking all DAG's status",
    schedule_interval=timedelta(days=1),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=find_betweeness
        
    )
task1 






