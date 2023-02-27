import json
import numpy as np
import pandas as pd
import re

def column_transformer_for_bq(df):
    df = pd.DataFrame(df)
    df.columns = df.columns.str.replace('[^A-Za-z0-9\s]+', '').str.lower().str.replace(' ', '_')

    return json.loads(df.to_json(orient='records'))


def _transformer_header(ti, **kwargs):
    import os.path
    
    dfs = []
    dag_folder = os.path.dirname(__file__)
    if 'transformation' not in dag_folder:
        dag_folder = os.path.join(dag_folder, 'transformation')

    json_file_path = os.path.join(dag_folder, 'event_columns_to_replace.json')
    df_json_objects, events = ti.xcom_pull(key='data', task_ids='gsheet_to_json_object')
    
    with open(json_file_path, 'r') as f:
        event_columns_to_replace = json.load(f)
    
    # Define the regex patterns to drop certain columns
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

def _transformer_data_enrichment(ti, **kwargs):
    """ Data Enrichment """

    dfs = ti.xcom_pull(key='event_header_cleansed', task_ids='transformer_header')
    dfs = [pd.DataFrame(df) for df in dfs]

    # Concat dataframes to optimize the transformation
    df = pd.concat(dfs).reset_index(drop=True)


    def get_email_domain(df):
        """ Get email domain, if KeyError create a new column with NaN """
        try:
            df['Email Domain'] = df['Email'].apply(
                lambda x: str(x).split('@')[-1] if not pd.isna(x) else "NA"
            )
        except KeyError:
            df['Email Domain'] = "NA"
        
        return df
    
    def get_student_year(df):
        """Get student year, if KeyError create a new column with NaN"""
        try:
            df['Tahun Angkatan Peserta'] = df['NPM'].apply(
                lambda x: str(x)[:2] if not pd.isna(x) else "0"
            )
        except KeyError:
            df['Tahun Angkatan Peserta'] = "0"
        
        return df

    # Add event year
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Tahun Acara'] = df['Timestamp'].apply(lambda x: x.year)
    
    df = get_student_year(df)
    df = get_email_domain(df)

    df['Timestamp'] = df['Timestamp'].astype(str)
    ti.xcom_push(key='event_data_cleansed', value=df.to_dict(orient='records'))

    return 'Event data enrichment Transformer'

def _transformer_hide_pii(ti, **kwargs):
    """ Hide Personal Identifiable Information (PII) """

    df = pd.DataFrame(
        ti.xcom_pull(key='event_data_cleansed', task_ids='transformer_data_enrichment')
    )

    def hide_student_id(df):
        """ Hide student ID (NPM), if KeyError create a new column with NaN """
        try:
            df['NPM'] = df['NPM'].apply(
                lambda x: str(x)[:2] + '*' * (len(str(x)) - 3) + str(x)[-1] if not pd.isna(x) else "0"
            )
        except KeyError:
            df['NPM'] = "0"

        return df

    def hide_student_name(df):
        """ Hide student name, if KeyError create a new column with NaN"""
        try:
            df['Nama Lengkap'] = df['Nama Lengkap'].apply(
                lambda x: str(x)[0] + '*' * (len(str(x)) - 2) + str(x)[-1] if not pd.isna(x) else "NA"
            )
        except KeyError:
            df['Nama Lengkap'] = "NA"

        return df

    def hide_email_username(df):
        """ Hide email username, if KeyError create a new column with NaN """
        try:
            df['Email'] = df['Email'].apply(
                lambda x: str(x).split('@')[0][0]
                + '*' * (len(str(x).split('@')[0]) - 2) 
                + str(x).split('@')[0][-1] if not pd.isna(x) else "NA"
            )
        except KeyError:
            df['Email'] = "NA"

        return df

    df = hide_student_id(df)
    df = hide_student_name(df)
    df = hide_email_username(df)

    ti.xcom_push(key='event_data_cleansed', value=df.to_dict(orient='records'))
    
    return 'Event data PII Transformer'

def _transformer_data_cleansing(ti, **kwargs):
    """ Event data cleansing """

    df = pd.DataFrame(
        ti.xcom_pull(key='event_data_cleansed', task_ids='transformer_hide_pii')
    )

    # Drop duplicate rows
    df = df.drop_duplicates(subset=df.drop(['Timestamp'], axis=1).columns, keep='last')

    # Drop empty string columns header
    try:
        df = df.drop(columns='')
    except KeyError:
        pass
    
    # Events
    # try:
    #     df['Event'] = df['Event'].replace(regex={r'.*OFFLINE.*': 'Talkshow Silogy Fest'})
    # except KeyError:
    #     df['Event'] = "NA"

    # Pekerjaan
    try:
        df['Pekerjaan'] = df['Pekerjaan'].replace({
            'Guru': 'Dosen/Guru'
            , 'Dosen': 'Dosen/Guru'
            , 'Lainnya': 'Umum'
        })
    except KeyError:
        df['Pekerjaan'] = "NA"

    # Program Studi

    try:
        ## Remove special characters
        df['Program Studi'] = df['Program Studi'].replace('[^A-Za-z0-9]+', ' ', regex=True)

        ## Change case to lower and strip whitespaces
        df['Program Studi'] = df['Program Studi'].str.lower().str.strip()

        ## Replace values and change case to title
        df['Program Studi'] = df['Program Studi'].replace(
            regex={
                r'.*siste(m?) infor(a?)ma(s?)i$|teknik informasi|ti': 'teknik informatika'
                , r'.*te[kh]nik inf(or|ro)matika$': 'teknik informatika'
                , r'.*teknik (elektro$|elektro.*(\d$))': 'teknik elektro'
                , r'^\s*$|^unsika$': "NA"
                , r's1 pendidikan agama islam': 'pendidikan agama islam'
                , r'informatika komputer': 'informatics computer'
                , r'rpl': 'rekayasa perangkat lunak'
                , r's1 informatika': 'informatika'
                , r's1 ilmu hukum': 'ilmu hukum'
                , r'd3 akuntansi': 'akuntansi'
                , r'd3kebidanan': 'kebidanan'
            }
        ).str.title()
    except KeyError:
        df['Program Studi'] = "NA"

    # Instansi

    try:
        ## Remove special characters
        df['Instansi'] = df['Instansi'].replace('[^A-Za-z0-9]+', ' ', regex=True)

        ## Change case to lower and strip whitespaces
        df['Instansi'] = df['Instansi'].str.lower().str.strip()

        ## Replace abbreviations
        df['Instansi'] = df['Instansi'].apply(
            lambda x: "NA" if type(x) is not 'str' else x.replace('univ ', 'universitas ')
        )

        df['Instansi'] = df['Instansi'].replace(regex={
            r'(.*unsika.*|.*si[nb]g(a?).*)': 'universitas singaperbangsa karawang',
            r'(fakultas ilmu komputer teknik informatika|teknik informatika|himsika|himtika|ilmu hukum|kebidanan|akuntansi|universitas)': 'universitas singaperbangsa karawang',
            r'(ubp|ubp karawang|ubp karawang teknik informatika|universitas buana perjuangan|universitas buana perjuangan karawanh|universitas buana karawang|unibersitas buana perjuangan karawang|unniversitas buana perjuangan karawang|karawang)': 'universitas buana perjuangan karawang',
            r'(.*unm.*|.*negeri makassar.*)': 'universitas negeri makassar',
            r'.*islam nusantara.*': 'universitas islam nusantara',
            r'.*wahid hasyim.*': 'universitas wahid hasyim',
            r'.*gu.*darma.*': 'universitas gunadarma',
            r'.*binaniaga.*': 'universitas bina niaga indonesia',
            r'.*unikom.*': 'universitas komputer indonesia',
            r'.*telkom.*': 'telkom university',
            r'.*mercu.*': 'universitas mercu buana',
            r'.*lp3i.*': 'lp3i college karawang',
            r'.*smi.*': 'politeknik sukabumi',
            r'ubsi': 'universitas bina sarana informatika',
            r'upi': 'universitas pendidikan indonesia',
            r'smk n 7 semarang': 'SMKN 7 Semarang',
            r'^\s*$|mahasiswa|umum|tidak bekerja|programmer beginner|payment gateway|informatika|sistem informasi|karawang': "NA"
        })

        ## Change case to title and upper for abbreviations
        df['Instansi'] = df['Instansi'].str.title()

        def to_approriate_case(x):
            if x is "NA":
                return "NA"

            tmp = x.split(' ')
            
            for i in range(len(tmp)):
                found = re.search(
                    r'(?!ng[skg])(?!ggl)(?!nch)(?!chm)(([^aiueoAIUEO ]){3}|(I[tp]b)|(Pt)|(Pepb)|(It)|(Ubd)|(Sman))'
                    , tmp[i]
                )
                
                if found:
                    tmp[i] = tmp[i].upper()

            return ' '.join(tmp)

        df['Instansi'] = df['Instansi'].apply(lambda x: to_approriate_case(x))
    except KeyError:
        df['Instansi'] = "NA"

    df_dict = df.to_dict('records')
    events = df['Event'].unique().tolist()

    ti.xcom_push(key='event_data_cleansed', value=[df_dict, events])

    return 'Event data cleansing Transformer'
