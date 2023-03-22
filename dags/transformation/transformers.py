import json
import numpy as np
import pandas as pd
import re

def column_transformer_for_bq(df):
    df = pd.DataFrame(df)
    df.columns = df.columns.str.replace('[^A-Za-z0-9\s]+', '').str.lower().str.replace(' ', '_')

    return json.loads(df.to_json(orient='records'))

def _transformer_header(ti, **kwargs):
    import json
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

    # Define mandatory columns
    # Load from file `transformation/mandatory_columns.json` into a dictionary
    json_file_path = os.path.join(dag_folder, 'mandatory_columns.json')
    with open(json_file_path, 'r') as f:
        mandatory_columns = json.load(f)
    
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

        for key, val in mandatory_columns.items():
            if key not in df.columns:
                df[key] = "NA" if val == 'object' else np.NaN

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
        """ Get email domain, if KeyError create a new column with NA """
        try:
            df['Email Domain'] = df['Email'].apply(
                lambda x: str(x).split('@')[-1] if not pd.isna(x) else "NA"
            )
        except KeyError:
            df['Email Domain'] = "NA"
        
        return df
    
    def get_student_year(df):
        """ Get 'student year' from 'email' that contain 'student.unsika'
        , if KeyError create a new column with 0, if error create a new column with 0 """

        def _get_year(x):
            if x is np.NaN or 'student.unsika' not in x:
                return np.NaN

            try:
                return int("20" + x[:2])
            except ValueError:
                print("Trying different way to extract year from email: {}".format(x))

                return int("20" + x.split('@')[0][-5:-3])

        try:
            df['Tahun Angkatan Peserta'] = df['Email'].apply(
                lambda x: _get_year(x)
            )
        except KeyError:
            df['Tahun Angkatan Peserta'] = 0
        
        return df

    # Add event year
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Tahun Acara'] = df['Timestamp'].apply(lambda x: x.year)
    
    df = get_email_domain(df)
    df = get_student_year(df)

    df['Timestamp'] = df['Timestamp'].astype(str)
    ti.xcom_push(key='event_data_cleansed', value=df.to_dict(orient='records'))

    return 'Event data enrichment Transformer'

def _transformer_hide_pii(ti, **kwargs):
    """ Hide Personal Identifiable Information (PII) """

    df = pd.DataFrame(
        ti.xcom_pull(key='event_data_cleansed', task_ids='transformer_data_enrichment')
    )

    def hide_student_id(df):
        """ Hide student ID (NPM), if KeyError create a new column with NA """
        try:
            df['NPM'] = df['NPM'].apply(
                lambda x: str(x)[:2] + '*' * (len(str(x)) - 3) + str(x)[-1]
                if not pd.isna(x) else "NA"
            )
        except KeyError:
            df['NPM'] = "NA"

        return df

    def hide_student_name(df):
        """ Hide student name, if KeyError create a new column with NA"""
        try:
            df['Nama Lengkap'] = df['Nama Lengkap'].apply(
                lambda x: str(x)[0] + '*' * (len(str(x)) - 2) + str(x)[-1]
                if not pd.isna(x) else "NA"
            )
        except KeyError:
            df['Nama Lengkap'] = "NA"

        return df

    def hide_email_username(df):
        """ Hide email username, if KeyError create a new column with NA """
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

        ## Replace values
        df['Program Studi'] = df['Program Studi'].replace(regex={
                r'.*siste(m?) infor(a?)ma(s?)i$': 'sistem informasi'
                , r'.*te[kh]nik inf(or|ro)matika$': 'teknik informatika'
                , r'.*teknik (elektro$|elektro.*(\d$))': 'teknik elektro'
        })

        df['Program Studi'] = df['Program Studi'].replace({
                's1 pendidikan agama islam': 'pendidikan agama islam'
                , 'informatika komputer': 'informatics computer'
                , 'teknik informasi': 'teknik informatika'
                , 'rpl': 'rekayasa perangkat lunak'
                , 's1 informatika': 'informatika'
                , 's1 ilmu hukum': 'ilmu hukum'
                , 'd3 akuntansi': 'akuntansi'
                , 'd3kebidanan': 'kebidanan'
                , 'ti': 'teknik informatika'
        })

        df['Program Studi'] = df['Program Studi'].replace(
            ['', 'unsika'], "NA"
        )
        
        # Change case to title
        df['Program Studi'] = df['Program Studi'].str.title()

    except KeyError:
        df['Program Studi'] = "NA"

    # Instansi

    try:
        def fix_univ_abbrev(df):
            """ Fix university abbreviations """

            df['Instansi'] = df['Instansi'].apply(          
                lambda x: x.replace('univ ', 'universitas ') if type(x) is str else x
            )
        
            return df

        ## Remove special characters
        df['Instansi'] = df['Instansi'].replace('[^A-Za-z0-9]+', ' ', regex=True)

        ## Change case to lower and strip whitespaces
        df['Instansi'] = df['Instansi'].str.lower().str.strip()

        ## Fix university abbreviations
        df = fix_univ_abbrev(df)

        ## Replace values
        df['Instansi'] = df['Instansi'].replace(
            '(.*unsika.*|.*si[nb]g(a?).*)', 'universitas singaperbangsa karawang', regex=True
        )

        df.loc[
            (df['Instansi'] == 'fakultas ilmu komputer teknik informatika') |
            (df['Instansi'] == 'teknik informatika') |
            (df['Instansi'] == 'himsika') |
            (df['Instansi'] == 'himtika') |
            (df['Instansi'] == 'ilmu hukum') |
            (df['Instansi'] == 'kebidanan') |
            (df['Instansi'] == 'akuntansi') |
            (df['Instansi'] == 'universitas')
            , ['Instansi']
        ] = 'universitas singaperbangsa karawang'

        df.loc[
            (df['Instansi'] == 'ubp') |
            (df['Instansi'] == 'ubp karawang') |
            (df['Instansi'] == 'ubp karawang teknik informatika') |
            (df['Instansi'] == 'universitas buana perjuangan') |
            (df['Instansi'] == 'universitas buana perjuangan karawanh') | 
            (df['Instansi'] == 'universitas buana karawang') | 
            (df['Instansi'] == 'unibersitas buana perjuangan karawang') | 
            (df['Instansi'] == 'unniversitas buana perjuangan karawang') |
            ((df['Instansi'] == 'karawang') & (df['Email Domain'] == 'mhs.ubpkarawang.ac.id'))
            , ['Instansi']
        ] = 'universitas buana perjuangan karawang'

        df['Instansi'] = df['Instansi'].replace(regex={
                r'(.*unm.*|.*negeri makassar.*)': 'universitas negeri makassar'
                , r'.*islam nusantara.*': 'universitas islam nusantara'
                , r'.*wahid hasyim.*': 'universitas wahid hasyim'
                , r'.*gu.*darma.*': 'universitas gunadarma'
                , r'.*binaniaga.*': 'universitas bina niaga indonesia'
                , r'.*unikom.*': 'universitas komputer indonesia'
                , r'.*telkom.*': 'telkom university'
                , r'.*mercu.*': 'universitas mercu buana'
                , r'.*lp3i.*': 'lp3i college karawang'
                , r'.*smi.*': 'politeknik sukabumi'
                , r'ubsi': 'universitas bina sarana informatika'
                , r'upi': 'universitas pendidikan indonesia'
                , r'smk n 7 semarang': 'SMKN 7 Semarang'
        })

        df['Instansi'] = df['Instansi'].replace(
            [
                '', 'mahasiswa', 'umum', 'tidak bekerja'
                , 'programmer beginner', 'payment gateway'
                , 'informatika', 'sistem informasi', 'karawang'
            ]
            , "NA"
        )

        ## Change case to title
        df['Instansi'] = df['Instansi'].str.title()

    except KeyError:
        df['Instansi'] = "NA"

    # Change float to int
    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].fillna(0)
            df[col] = df[col].astype('int64')

    print(df.info())

    df_dict = df.to_dict('records')
    events = df['Event'].unique().tolist()

    ti.xcom_push(key='event_data_cleansed', value=[df_dict, events])

    return 'Event data cleansing Transformer'
