import pandas as pd
import aiohttp
import asyncio
import streamlit as st
import networkx as nx
from networkx.algorithms import community
from datetime import datetime


st.header('Clustering App')


api_key = st.text_input(label='Aves API key:',max_chars=28)

mode = st.radio(
    "Which format do you want to input your data?",
    ('Text', 'CSV'))

if mode == 'Text':
    input_text = st.text_area(label='Enter keywords:',placeholder='''Keyword1\nKeyword2''',height=200)
    query_list = input_text.split('\n')

else:
    try:
        uploaded_file = st.file_uploader("Choose your file:",)
        all_df = pd.read_csv(uploaded_file)
        headers = all_df.columns.to_list()
        keyword_col = st.selectbox(label='Select KEYWORD column name:', options=headers)
        query_list = list(all_df[keyword_col].unique())
    except:
        pass



list_end = len(query_list)
st.write(f"we found {len(query_list)} rows!")
st.write(query_list[:10])


sameUrl = st.slider('How many URLs do you want to pair?',0,10,6)  


step = 5000

final_df = pd.DataFrame()

if st.button('Start'):
    for start in range(0,list_end+1,step):
        stop = start+step
        if stop>list_end:
            stop = list_end
        keyword_list = query_list[start:stop]
        async def main():
            temp_df = pd.DataFrame(columns=['query','link','title','position'])
            session_timeout =  aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(timeout=session_timeout,trust_env=True) as session:
                tasks = []
                for kw in keyword_list:
                    task = asyncio.ensure_future(get_data(session,kw))
                    tasks.append(task)

                result = await asyncio.gather(*tasks)
            for item in result:
                try:
                    queryName = item['search_parameters']['query']
                    # print(queryName)
                    organic_df = pd.json_normalize(item['result']['organic_results'])
                    organic_df['query'] = queryName
                    organic_df = organic_df.reindex(columns=['query','url','title','position']).rename(columns={'url':'link'})
                    temp_df = pd.concat([temp_df,organic_df])
                except Exception as e:
                    print(e)
                    pass
            return temp_df

            

        async def get_data(session, keyword):
            url = f'https://api.avesapi.com/search?apikey={api_key}&type=web&query={keyword}&google_domain=google.com&gl=ir&hl=fa&device=desktop&output=json&num=100'
            async with session.get(url,ssl=True) as resp:
                result_data = await resp.json()
                return result_data

        text = st.subheader('Sending request. It might take a while, So please wait!')
        temp_df = asyncio.run(main())
        final_df = pd.concat([final_df,temp_df])
    
    text.subheader('Clustering Started!')
    keyword_list = final_df['query'].drop_duplicates().to_list()
    data_frame_google = final_df[final_df['position']<11]
    join_df = pd.merge(data_frame_google,data_frame_google,on="link",how="inner")
    grouped_df = join_df.groupby(['query_x', 'query_y']).nunique()
    grouped_df = grouped_df[grouped_df.link>=sameUrl]
    

    group_dict = grouped_df.to_dict()

    G = nx.Graph()
    nodes = tuple(keyword_list)
    G.add_nodes_from(nodes)

    edge_list = []
    for i in group_dict['link']:
        edge_list.append(i)
    G.add_edges_from(edge_list)

    # community detection
    com = community.greedy_modularity_communities(G)

    clusters_dict_list = []
    for i in com:
        temp_cluster = list(i)
        temp_cluster = sorted(temp_cluster,reverse=True)
        for j in temp_cluster:
            clusters_dict_list.append({'query':j,'cluster':temp_cluster[0]})

    cluster_df =  pd.DataFrame(clusters_dict_list)
    total_df = pd.merge(final_df,cluster_df,on='query',how='left')
    
    text.subheader('Done! You can download it now:')
    
    @st.cache
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv(index=False).encode('utf-8')

    csv = convert_df(total_df)
    today = datetime.today()
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name=f'clustering-{str(today)}.csv',
        mime='text/csv',
    )
