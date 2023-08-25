from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
local_tz = timezone('Asia/Kolkata')



import re
import psycopg2
import networkx as nx
import networkx.algorithms.community as nx_comm
import pandas as pd
import numpy as np
import psycopg2
import operator
from sqlalchemy import create_engine
from pandasql import sqldf


def main(engine):
    try:

        query = "select distinct lower(from_node) as from_node,lower(to_node) as to_node,route_type as hirearchy,network_type from bbip_route_details"
        queryResult = pd.read_sql(query,engine)

        bbip_route_details = pd.read_sql(query,engine)
        queryResult = queryResult[['from_node','to_node']]


        graph = []

        G = nx.DiGraph()
        #G = nx.MultiDiGraph()

        for _,edge in queryResult[['from_node','to_node']].iterrows():
            G.add_edge(edge['from_node'],edge['to_node'])

        nx.number_of_nodes(G)

        nx.is_connected(G.to_undirected())

        con = list(nx.connected_components(G.to_undirected()))
        n_grafo_principal = max(len(c) for c in con)
        #print(n_grafo_principal)
        for n, c in enumerate(con):
            #print(len(c))
            if len(c) < n_grafo_principal:
                G.remove_nodes_from(con[n])

        nx.is_connected(G.to_undirected())

        #print('edges:', nx.number_of_edges(G), 'nodes:', nx.number_of_nodes(G))

        G_bridge = (G.to_undirected()).copy()

        bridges = list(nx.bridges(G_bridge))


        pontes = []
        n_nos = G_bridge.number_of_nodes()
        for bridge in bridges:
            G_temp = G_bridge.copy()
            G_temp.remove_edge(*bridge)
            con = list(nx.connected_components(G_temp))
            h = max([len(c) for c in con])
            pontes.append([bridge, n_nos-h])

        pontes = sorted(pontes, key=lambda x: x[1], reverse=True)
        #pontes

        single_df = pd.DataFrame(pontes, columns=['Tuple', 'Value'])
        single_df[['from_node', 'to_node']] = pd.DataFrame(single_df['Tuple'].tolist())
        single_df[['prior_state']] = pd.DataFrame(single_df['Value'].tolist())
        single_df = single_df.drop(['Tuple', 'Value'], axis=1)
        single_df['current_state'] = 'single failure'


        # remove as bridges:
        G_no_bridge = G_bridge.copy()
        G_no_bridge.remove_edges_from(bridges)

        # remove os nós que ficaram isolados:
        con = list(nx.connected_components(G_no_bridge))
        n_grafo_principal_2 = max(len(c) for c in con)
        for n, c in enumerate(con):
            if len(c) < n_grafo_principal_2:
                G_no_bridge.remove_nodes_from(con[n])
        nx.is_connected(G_no_bridge)

        #print(G_bridge.number_of_nodes(), G_no_bridge.number_of_nodes())
        100-(G_no_bridge.number_of_edges())/G_bridge.number_of_edges()*100

        pairs = []
        con_grafo_principal = list(nx.connected_components(G_bridge))
        n_grafo_principal = max([len(con_grafo_principal[x]) for x in range(0,len(con_grafo_principal))])
        for e in G_no_bridge.edges:
            G_temp=G_no_bridge.copy()
            try:
                G_temp.remove_edge(*e) # Remove uma aresta
            except:
                continue
            bridges2 = list(nx.bridges(G_temp)) # encontra as bridges
            if len(bridges2) != 0:
                for b in bridges2:
                    if [b,e] not in [pair[0:2] for pair in pairs]:
                        G_temp2 = G_bridge.copy()
                        G_temp2.remove_edges_from([e,b])
                        con = list(nx.connected_components(G_temp2))
                        h = max([len(con[x]) for x in range(0,len(con))])
                        pairs.append([e,b,n_grafo_principal-h])

        pairs = sorted(pairs, key=lambda par:par[2], reverse= True)

        pairs_critical = [pair for pair in pairs if pair[2]>0]

        split_data = []

        for item in pairs_critical:
            pair1 = item[0]
            pair2 = item[1]
            value = item[2]
            
            split_data.append((pair1[0], pair1[1], value))
            split_data.append((pair2[0], pair2[1], value))

        double_df = pd.DataFrame(split_data, columns=['from_node', 'to_node', 'prior_state'])
        double_df['current_state'] = 'double failure'


        df = pd.concat([single_df,double_df])

        query = "select distinct a.from_node as from_node,a.to_node,a.prior_state,a.current_state,b.hirearchy,network_type from df a left join bbip_route_details b on lower(a.from_node)= lower(b.from_node)"
        df = sqldf(query)

        return df

    except Exception as e:

        print("Someting Went Wrong :", str(e))
       


def store(df,engine,cursor,connection):

    try:

        cursor.execute("TRUNCATE TABLE route_impact_analysis_disjunction")

        connection.commit()

        print("Table truncated successfully.")

        df.to_sql('route_impact_analysis_disjunction',engine,if_exists='append',index=False)

        print("Table Inserted Successfully")

    except Exception as e:

        print("Someting Went Wrong :", str(e))

def postgres_conn():

    params = {
        'host': "localhost",
        'port': "5432",
        'database': "uc12",
        'user': "postgres",
        'password': "131120"
    }

#     params = {
#         'host': "telco-postgresql-mvp-primary.telco-datastorage-mvp.svc.cluster.local",
#         'port': "5432",
#         'database': "heatmap",
#         'user': "postgres",
#         'password':"admintelco100"
#     }

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

    return engine ,connection, cursor

       
def find_disjunction():

    engine, connection , cursor = postgres_conn()
    
    disjunctionDF = main(engine)

    store(disjunctionDF,engine,cursor,connection)    

    engine.dispose() 
    cursor.close()
    connection.close()

    print("close connection")




default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,6,12)),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'catchup': False,

}

with DAG(
    dag_id='backbone_disjunction_',
    default_args=default_args,
    description="Checking all DAG's status",
    schedule_interval=timedelta(minutes=15),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=find_disjunction,
        
    )
task1 
