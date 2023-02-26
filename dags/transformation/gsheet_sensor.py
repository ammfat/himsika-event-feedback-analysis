def _gsheet_sensor(ti, **kwargs):
    from googleapiclient.discovery import build

    gdrive_service = build('drive', 'v3', credentials=CREDENTIALS)

    search_start_date = datetime.strptime(kwargs.get('ts'), '%Y-%m-%dT%H:%M:%S%z') - timedelta(days=7)
    search_end_date = datetime.strptime(kwargs.get('ts'), '%Y-%m-%dT%H:%M:%S%z')
    search_start_date = search_start_date.strftime('%Y-%m-%dT%H:%M:%S')
    search_end_date = search_end_date.strftime('%Y-%m-%dT%H:%M:%S')

    tmp_sheets = []
    pageToken = ""

    while pageToken is not None:
        response = gdrive_service.files().list(
            q=f"""
                mimeType='application/vnd.google-apps.spreadsheet'
                and not name contains 'output'
                and createdTime >= '{ search_start_date }'
                and createdTime <= '{ search_end_date }'
            """
            , fields="nextPageToken, files(id, name, owners, createdTime, modifiedTime)"
        ).execute()

        tmp_sheets.extend(response.get('files', []))

        pageToken = response.get('nextPageToken')

    sheets = [sheet for sheet in tmp_sheets if not sheet['owners'][0]['me']]
    
    ti.xcom_push(key='date', value=[search_start_date, search_end_date])
    ti.xcom_push(key='sheets', value=sheets)

if __name__ == "__main__":
    _gsheet_sensor(ti)