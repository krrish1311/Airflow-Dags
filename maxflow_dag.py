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
import argparse
from pandasql import sqldf

class MaxFlow_class:
    def __init__(self,source,target,created_by,network_type):
        self.source = source
        self.target = target
        self.created_by = created_by
        self.network_type = network_type
       

    def maxflow(self,engine,network_type):

        query = f"SELECT exit_route, lower(from_node) as from_node, concat(lower(from_node), '|', lower(from_interface)) as to_node, from_isis, regexp_replace(capacity, '[^0-9]+', '', 'g') as capacity,lower(from_interface) as from_interface, lower(to_interface) as to_interface, sentido as traffic,a.network_type,b.pct as traffic_capacity FROM bbip_route_details a left join cacti_usage_interfaces b on lower(a.from_node) = lower(b.inthost) and lower(a.from_interface) = lower(b.intname) where network_type = '{network_type}' and from_isis > '0' and to_isis > '0' union SELECT exit_route, concat(lower(from_node), '|', lower(from_interface)) as from_node, concat(lower(to_node), '|', lower(to_interface)) as to_node, from_isis, regexp_replace(capacity, '[^0-9]+', '', 'g') as capacity,lower(from_interface) as from_interface, lower(to_interface) as to_interface, sentido as traffic, a.network_type,b.pct as traffic_capacity FROM bbip_route_details a left join cacti_usage_interfaces b on lower(a.from_node) = lower(b.inthost) and lower(a.from_interface) = lower(b.intname) where network_type = '{network_type}' and from_isis > '0' and to_isis > '0' union SELECT exit_route, concat(lower(to_node), '|', lower(to_interface)) as from_node, to_node, from_isis, regexp_replace(capacity, '[^0-9]+', '', 'g') as capacity,lower(from_interface) as from_interface, lower(to_interface) as to_interface, sentido as traffic, a.network_type,b.pct as traffic_capacity FROM bbip_route_details a left join cacti_usage_interfaces b on lower(a.from_node) = lower(b.inthost) and lower(a.from_interface) = lower(b.intname) where network_type = '{network_type}' and from_isis > '0' and to_isis > '0'"

        routers = pd.read_sql(query,engine)

        traffic_capacity_df = pd.read_sql(query,engine)

        # print(network_type,traffic_capacity_df[['from_node','to_node']])
        

        routers = routers[['from_node','to_node','from_isis','capacity']]

        #routers

        routers['from_node'] = routers['from_node'].astype(str)
        routers['to_node'] = routers['to_node'].astype(str)
        routers['from_isis'] = routers['from_isis'].astype(float)
        #routers['capacity'] = routers['capacity'].astype(float)

        routers['capacity'] = routers['capacity'].replace('', '0').astype(float)



        routers['from_node'] = routers['from_node'].apply(lambda x: x.lower())
        routers['to_node'] = routers['to_node'].apply(lambda x: x.lower())
        
        routers['link_name'] = routers.apply(lambda row: '_'.join(sorted([row['from_node'],row['to_node']])),axis=1)
        routers.drop_duplicates(subset=['link_name'], inplace=True)
        #routers.head()


        routers['bw'] = routers.apply(lambda row: row['from_isis'], axis=1)
        routers['bw_inv'] = routers['bw'].apply(lambda x: 1/x)
        #print(routers)

        s1 = routers['from_node']
        s2 = routers['to_node']
        s3 = pd.concat([s1,s2])
        s3 = s3.unique()


        # routers['from_node'].apply(lambda x:( 
        #                                      'HL1' if 'hl1' in x.lower() 
        #                                 else 'HL2' if 'hl2' in x.lower() 
        #                                 else 'HL3' if 'hl3' in x.lower() 
        #                                 else 'HL4' if 'hl4' in x.lower() 
        #                                 else 'HL5' if 'hl5' in x.lower() 
        #                                 else 'OTHERS')).value_counts()

        # routers['to_node'].apply(lambda x:( 
        #                                      'HL1' if 'hl1' in x.lower() 
        #                                 else 'HL2' if 'hl2' in x.lower() 
        #                                 else 'HL3' if 'hl3' in x.lower() 
        #                                 else 'HL4' if 'hl4' in x.lower() 
        #                                 else 'HL5' if 'hl5' in x.lower() 
        #                                 else 'OTHERS')).value_counts()


        

        from sklearn.preprocessing import MinMaxScaler, StandardScaler
        scaler = MinMaxScaler()
        routers['bw_norm'] = scaler.fit_transform(routers['bw'].values.reshape(-1, 1))
        routers['bw_norm_inv'] = routers['bw_norm'].apply(lambda x: 1-x)
        routers['bw_norm_inv'] = routers['bw_norm'].apply(lambda x: -1*x)

        routers_bbip = routers[routers['from_node'].isin(s3) & routers['to_node'].isin(s3)]
        #print(routers_bbip)

        import matplotlib.pyplot as plt
        import matplotlib.cm as cm


        J = nx.Graph()

        for node in s3:
            J.add_node(node)


        for _,edge in routers_bbip[['from_node','to_node','bw_norm_inv','bw_norm','from_isis','capacity']].iterrows():
            J.add_edge(edge['from_node'],edge['to_node'], weight=edge['bw_norm_inv'],capacity=edge['capacity'], metric=edge['from_isis'])


        idx_small_c = [i for i, g in enumerate(list(nx.connected_components(J))) if len(g) < 10]

        el_small_c = []
        for i in idx_small_c:
          g = list(list(nx.connected_components(J))[i])
          el_small_c.extend(g)

        for el in el_small_c:
          J.remove_node(el)


        # communities = nx_comm.louvain_communities(J, weight='metric', resolution=0.075)


        # node_color = []
        # for node in J.nodes():
        #     node_color.append([i for i, comm in enumerate(communities) if node in comm][0])

        # pos = nx.spring_layout(J)
        # plt.figure(figsize=(25, 25))
        # nx.draw(J, pos=pos, node_size=60, width=0.5, node_color=node_color, cmap=cm.get_cmap("Paired"), vmin=min(node_color), vmax=max(node_color))



        # sources = [n for n in J.nodes() if 'hl1' in n]
        # targets = [n for n in J.nodes() if 'hl1' not in n]
        # btw_subset = nx.edge_betweenness_centrality_subset(J, targets=targets, sources=sources, normalized=True)
        # #tw_subset = nx.betweenness_centrality_subset(J, targets=targets, sources=sources, weight='metric', normalized=True)
        # btw_subset = {k: v for k, v in sorted(btw_subset.items(), key=lambda item: item[1], reverse=True)}
        # btw_subset1 = btw_subset
        # btw_subset


        # pos = nx.spring_layout(J)
        # plt.figure(figsize=(25, 25))
        # node_color = [('red' if 'hl1' in n else 'blue') for n in J.nodes()]
        # node_size = [(120 if 'hl1' in n else 60) for n in J.nodes()]
        # edge_color = [100*btw_subset[e] for e in J.edges()]


        # nx.draw(J, 
        #         pos=pos, 
        #         width=[1000*btw_subset[e] for e in J.edges()], 
        #         node_color=node_color,
        #         edge_color=edge_color, 
        #         node_size=node_size, 
        #         cmap= plt.cm.Reds, 
        #         vmin=min(edge_color), vmax=max(edge_color))


        #flow_norm = nx.maximum_flow(J, _s='i-br-ce-fla-ad-hl2-01', _t='i-br-rj-rjo-gsr-hl2-01', capacity='capacity', flow_func=None)
        flow_norm = nx.maximum_flow(J, _s=self.source, _t=self.target, capacity='capacity', flow_func=None)


        #flow_norm
        data = []
        for source, targets in flow_norm[1].items():
            for dest, value in targets.items():
                data.append((source, dest, value))
        MaxFlow_df = pd.DataFrame(data, columns=['from_node', 'to_node', 'maxflow_prior_nw_impact'])

        #=================

        traffic_capacity_df = pd.DataFrame(traffic_capacity_df, columns=['from_node', 'to_node', 'capacity','traffic','network_type','traffic_capacity'])
        traffic_capacity_df[['from_node', 'from_interface']] = traffic_capacity_df['from_node'].str.split('|', expand=True)

        # Split MainColumn2 into two columns

        traffic_capacity_df[['to_node', 'to_interface']] = traffic_capacity_df['to_node'].str.split('|', expand=True)

        #print("fl:",traffic_capacity_df)

        #=================

        # Split MainColumn1 into two columns
        MaxFlow_df[['from_node', 'from_interface']] = MaxFlow_df['from_node'].str.split('|', expand=True)

        # Split MainColumn2 into two columns
        MaxFlow_df[['to_node', 'to_interface']] = MaxFlow_df['to_node'].str.split('|', expand=True)

        MaxFlow_df['created_by'] = self.created_by 
        MaxFlow_df['source'] = self.source
        MaxFlow_df['target'] = self.target

        final_query = "select distinct m.from_node, m.to_node, m.from_interface, m.to_interface, m.maxflow_prior_nw_impact, CAST(CASE WHEN r.capacity IS NOT NULL THEN r.capacity ELSE d.capacity END AS float) AS capacity, r.traffic,m.created_by,r.network_type,r.traffic_capacity,m.source,m.target from MaxFlow_df m left join( select * from traffic_capacity_df where from_interface is not null ) r on m.from_node = r.from_node and m.from_interface = r.from_interface left join ( select * from traffic_capacity_df where to_interface is not null ) d on m.to_node = d.to_node and m.to_interface = d.to_interface where maxflow_prior_nw_impact > 0"

        df = sqldf(final_query)

        #print("fl_qu:",df)

        return df

        

def store(df,engine,cursor,created_by,connection):

        try:
            # Execute the TRUNCATE statement
            #cursor.execute("TRUNCATE TABLE route_impact_analysis_maxflow")
            cursor.execute(f"DELETE FROM route_impact_analysis_maxflow WHERE created_by = '{created_by}'")
            # Commit the changes
            connection.commit()
            print("Particular Data Deleted Successfully :")

            df = df.drop_duplicates(subset=['from_interface', 'to_interface'])
            
            df.to_sql('route_impact_analysis_maxflow',engine,if_exists='append',index=False)

            print("Table Inserted Successfully")

            return df

        except Exception as e:
            # Handle the exception
            print("Error occurred while deleting the table:", str(e))
            # Rollback the changes
            connection.rollback()
                  

def find_maxflow():

    # params = {
    #             'host': "telco-postgresql-mvp-primary.telco-datastorage-mvp.svc.cluster.local",
    #             'port': "5432",
    #             'database': "heatmap",
    #             'user': "postgres",
    #             'password':"admintelco100"
    #         }

    params = {
                 'host': "localhost",
                 'port': "5432",
                 'database': "uc12",
                 'user': "postgres",
                 'password':"131120"
             }

    connection_string = f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"

    engine = create_engine(connection_string)

    connection = psycopg2.connect(host=params['host'],
                              port=params['port'],
                              database=params['database'],
                              user=params['user'],
                              password=params['password'])

    cursor = connection.cursor()


#     parser = argparse.ArgumentParser(description='Read a CSV file and store its contents in a MySQL database')
#     parser.add_argument('--source', type=str, required=True, help='sources name')
#     parser.add_argument('--target', type=str, required=True, help='target name')
#     parser.add_argument('--created_by', type=str, required=True, help='created by')
#     parser.add_argument('--backbone_type', type=str, required=True, help='enter backbone type')
#     args = parser.parse_args()
#     created_by = args.created_by

#     df = None  # Initialize df variable
    
#     backbone_type = args.backbone_type
#     if backbone_type == 'internet':

#         network_type = "internet backbone"
#         maxflow_obj = MaxFlow_class(args.source, args.target ,args.created_by,network_type) # create an object of 
#         df = maxflow_obj.maxflow() # call the maxflow() method

#     elif backbone_type == 'critical':

#         network_type = "critical backbone"
#         maxflow_obj = MaxFlow_class(args.source, args.target ,args.created_by,network_type) # create an object of 
#         df = maxflow_obj.maxflow() # call the maxflow() method

#     else:
#         print("Please select Network type")

    network_type = "internet backbone"  # Set the network type here
    maxflow_obj = MaxFlow_class('i-br-sp-spo-ib-hl2-01', 'i-br-sp-arq-afl-hl2-01' ,'Krrish',network_type) # create an object of 
    internet_df = maxflow_obj.maxflow(engine,network_type) # call the maxflow() method

    # network_type = "critical backbone"  # Set the network type here
    # maxflow_obj = MaxFlow_class('C-BR-RJ-RJO-TP-RAC-01', 'C-BR-SC-LGS-LGS-RAC-02' ,'Krrish',network_type) # create an object of 
    # critical_df = maxflow_obj.maxflow(engine,network_type) # call the maxflow() method
    
    # df=pd.concat([internet_df,critical_df])
  


    store(internet_df,engine,cursor,'Krrish',connection)

# Close the cursor and connection
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
    dag_id='maxflow_dag',
    default_args=default_args,
    description="Checking all DAG's status",
    schedule_interval=timedelta(days=1),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=find_maxflow
        
    )
task1 

