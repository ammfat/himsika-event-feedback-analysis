import json
import pandas as pd

def _event_column_transformer_for_bq(df):
    df = pd.DataFrame(df)
    df.columns = df.columns.str.replace('[^A-Za-z0-9\s]+', '').str.lower().str.replace(' ', '_')

    return json.loads(df.to_json(orient='records'))


def _event_header_transformer(ti, **kwargs):
    import os.path
    
    dfs = []
    dag_folder = os.path.dirname(__file__)
    if 'transformation' not in dag_folder:
        dag_folder = os.path.join(dag_folder, 'transformation')

    json_file_path = os.path.join(dag_folder, 'event_columns_to_replace.json')
    df_json_objects, events = ti.xcom_pull(key='data', task_ids='gsheet_to_json_object')
    
    with open(json_file_path, 'r') as f:
        event_columns_to_replace = json.load(f)
    
    # define the regex patterns to drop certain columns
    drop_patterns = ['[Ff]ollow', '[Mm]erge', 'no', '[Pp]resensi']

    for df, event in zip(df_json_objects, events):
        df = pd.DataFrame(df)

        print("Before transformation: ", df.columns)

        df['Event'] = event.title()
    
        for pattern in drop_patterns:
            df = df[df.columns.drop(list(df.filter(regex=pattern)))]
        
        df = df.rename(columns=event_columns_to_replace)

        tmp_cols = [col for col in df.columns if 'Pemaparan materi oleh' in col]
        if len(tmp_cols) > 0:
            df['Kepuasan terhadap Pemateri'] = df[tmp_cols].mean(axis=1).round()
            df = df[df.columns.drop(list(df.filter(like='Pemaparan materi oleh ')))]

        print("After transformation: ", df.columns)
        dfs.append(df.to_dict(orient='records'))

    ti.xcom_push(key='event_header_cleansed', value=dfs)

    return 'Event HEADER Transformer'


def _event_data_transformer(ti, **kwargs):
    dfs = ti.xcom_pull(key='event_header_cleansed', task_ids='event_header_transformer')
    dfs = [pd.DataFrame(df) for df in dfs]

    df = pd.concat(dfs).reset_index(drop=True)

    return 'Event Data Transformer'
