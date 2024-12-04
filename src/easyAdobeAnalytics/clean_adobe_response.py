import json
import datetime
import pandas as pd
import copy

# Total hours spent: 60

def explode_elements(df,depth,date,elems,metrics):
    df_breakdown = pd.DataFrame.from_records(df['breakdown'].dropna().tolist())
    all_data = []
    element_column = f'{elems[depth]["name"]}'
    for i in range(len(df_breakdown.columns)):
        df_bb = pd.json_normalize(df_breakdown[i])
        df_bb.rename(columns={'name':element_column},inplace= True)
        if 'breakdown' in df_bb.columns:
            df_bb.drop(columns=['breakdownTotal','url'],inplace=True)
            try:
                parent_element =  df_bb[element_column].iloc[0]
                df_bb_temp = explode_elements(df_bb,depth+1,parent_element,elems,metrics)
            except:
                parent_element=None
        else:
            df_bb.drop(columns=['url'],inplace=True)
            if 'counts' in df_bb.columns:
                df_bb = df_bb.explode('counts',ignore_index=True)
                df_bb['Metric Name'] = metrics
                df_bb['counts'] = pd.to_numeric(df_bb['counts'])
                
                df_pivot = df_bb.pivot_table(index=element_column, columns='Metric Name', values='counts')
                for key in ['Metric Name','counts','urls']:
                    try:
                        df_pivot = df_pivot.drop(columns=[key],inplace=True)
                    except KeyError:
                        pass
                df_bb_temp = df_pivot.reset_index()
        all_data.append(df_bb_temp.copy())
    try:
        all_data_df = pd.concat(all_data)
    except ValueError:
        return None
    if depth == 0:
        groupby_columns = []
        for elem in elems:
            groupby_columns.append(elem['name'])
        try:
            all_data_df = all_data_df.groupby(groupby_columns+metrics).sum().reset_index()
            all_data_df['Date'] = date
        except KeyError:
            all_data_df['Date'] = datetime.datetime(2077,1,1)
    else:
        all_data_df = all_data_df.groupby([element_column]+metrics).sum().reset_index()
        all_data_df[f'{elems[depth-1]["name"]}'] = date
    return all_data_df

def explode_no_elements(df,date,metrics):
    all_data = []
    df_bb = df.explode('counts',ignore_index=True)
    
    df_bb['Metric Name'] = metrics
    df_bb['counts'] = pd.to_numeric(df_bb['counts'])
    df_pivot = df_bb.pivot_table(columns='Metric Name', values='counts')
    df_bb_temp = df_pivot.reset_index(drop=False)
    
    all_data.append(df_bb_temp.copy())
    all_data_df = pd.concat(all_data)
    all_data_df = all_data_df.groupby(metrics).sum().reset_index()
    all_data_df['Date'] = date
    return all_data_df

def explode_with_stack(df,date,elems,metrics):
    df_breakdown = pd.DataFrame.from_records(df['breakdown'].dropna().tolist())
    all_data_function = []
    for i in range(len(df_breakdown.columns)):
        row_df = pd.DataFrame()
        row_df['Date'] = [date]
        stack = [(df_breakdown[i],0,row_df.copy())]
        while stack:
            df_bb,depth,parent_df = stack.pop()
            df_bb = pd.json_normalize(df_bb)
            df_bb.rename(columns={'name':elems[depth]["name"]},inplace= True)
            parent_df[elems[depth]["name"]] = df_bb[elems[depth]["name"]]
            if 'breakdown' in df_bb.columns:
                df_bb_breakdown = pd.DataFrame.from_records(df_bb['breakdown'].dropna().tolist())
                for j in range(len(df_bb_breakdown.columns)):
                    stack.append((df_bb_breakdown[j],depth+1,parent_df.copy()))
            elif depth == len(elems)-1:
                df_bb = df_bb.drop(columns=[elems[depth]["name"],"url"])
                df_bb = df_bb.explode('counts',ignore_index=True)
                df_bb['Metric Name'] = metrics
                df_bb['counts'] = pd.to_numeric(df_bb['counts'])
                
                df_bb = df_bb.pivot_table(columns='Metric Name', values='counts')

                for metric in metrics:
                    parent_df[metric] = df_bb[metric]['counts']
                all_data_function.append(parent_df.copy())

    try:
        all_data_function = pd.concat(all_data_function)
    except:
        None
    return all_data_function

def json_to_df(req,elements):
    elems = req['report'].get('elements',None)
    mets = req['report']['metrics']
    segments = req['report'].get('segments',None)
    metrics = [x['name'] for x in mets]
    dimensions = [x['name'] for x in elems]
    all_data = []
    for data in req['report']['data']:
        date = datetime.datetime(data['year'],data['month'],data['day'])
        df = pd.json_normalize(data)
        if elements:
            df.drop(columns=['year','month','day','name','breakdownTotal'],inplace=True)
            
            df_elements = explode_with_stack(df,date,elems,metrics)
            for elem in elems:
                try:
                    df_elements = df_elements.loc[df_elements[elem['name']].astype(str) != '0']
                except:
                    pass
        else:
            df.drop(columns=['year','month','day','name'],inplace=True)
            df_elements = explode_no_elements(df,date,metrics)

        for key in ["Metric Name","url","counts","breakdown","breakdownTotal","index"]:
            try:
                df_elements.drop(columns=[key],inplace=True)
            except (KeyError, AttributeError):
                pass     
        all_data.append(df_elements.copy())
    
    try:
        all_data_df = pd.concat(all_data)
    except:
        return None
    if not elements:
        all_data_df = all_data_df.groupby(['Date'] + metrics).sum().reset_index()
        for key in ["Metric Name","url","counts","breakdown","breakdownTotal","index"]:
            try:
                all_data_df.drop(columns=[key],inplace=True)
            except KeyError:
                pass

    index_columns = ['Date']
    if segments:
        all_data_df['Segments'] = req['report']['segments'][0]['name']
        index_columns.append('Segments')
    if elements:
        for elem in elems:
            index_columns.append(elem['name'])

    if metrics:
        all_data_df = all_data_df.loc[~all_data_df[metrics].eq(0).all(axis=1)]
        pass
    try:
        all_data_df.set_index(index_columns,inplace=True,drop=False)
        all_data_df['recursive_index'] = all_data_df.index
        return all_data_df
    except KeyError:
        return all_data_df