
from tzlocal import get_localzone
from datetime import datetime
import psycopg2
from sec_api import QueryApi
from decouple import config
import mapping
from celeryconfig import app

try:
    connection = psycopg2.connect(
        "dbname=celery_task user=myuser password=password host=localhost port=5432"
    )
    print("Connected successfully!")
    # connection.close()
except psycopg2.Error as e:
    print("Error:", e)

query_api = QueryApi(api_key=config('sec-api-key2'))

def create_tables():
    cur = connection.cursor()
    create_table_commands = (
        """
        CREATE TABLE filings(
            filing_id varchar(255) PRIMARY KEY,
            cik int,
            filer_name varchar(255),
            filed_at date
        )
        """,
        """
        CREATE TABLE holdings(
            filing_id varchar(255),
            name_of_issuer varchar(255),
            cusip varchar(255),
            title_of_class varchar(255),
            value bigint,
            shares int,
            put_call varchar(255)
        )
        """,
        """
        CREATE TABLE holding_infos(
            cusip varchar(255),
            security_name varchar(255),
            ticker varchar(50),
            exchange_code varchar(10),
            security_type varchar(50)
        )
        """
    )
    for command in create_table_commands:
        cur.execute(command)
    cur.close()
    connection.commit()

def save_to_db(filings):
    print("start")
    cursor = connection.cursor()
    
    for filing in filings:
        
        if not filing['holdings']:
            continue
            
        insert_commands = (
            """
            INSERT INTO filings(
                filing_id,
                cik,
                filer_name,
                filed_at
            ) VALUES(%s,%s,%s,%s)
            """,
            """
            INSERT INTO holdings(
                filing_id,
                name_of_issuer,
                cusip,
                title_of_class,
                value,
                shares,
                put_call
            ) VALUES(%s,%s,%s,%s,%s,%s,%s)
            """
        )

        filing_values = (
            filing['id'],
            filing['cik'],
            filing['companyName'],
            filing['filedAt']
        )
        cursor.execute(insert_commands[0],filing_values)

        #SAVING HOLDINGS
        for holding in filing['holdings']:
            holding_values = (
                filing['id'],
                holding['nameOfIssuer'],
                holding['cusip'],
                holding['titleOfClass'] if "titleOfClass" in holding else '',
                holding['value'],
                holding['shrsOrPrnAmt']['sshPrnamt'],
                holding['putCall'] if "putCall" in holding else '',
            )
            cursor.execute(insert_commands[1],holding_values)
    cursor.close()
    connection.commit()
    print("done")


def get_data(start_date,end_date):
    # [2023-08-30 TO 2023-08-31]
    # Get the local timezone
    local_timezone = get_localzone()

    # Get the current time in the local timezone
    current_time_in_local = datetime.now(local_timezone)

    # Get the timezone offset as "+05:30" format
    current_timezone_offset = current_time_in_local.strftime('%z')

    # print(current_timezone_offset)

    query = {
        "query": {
            "query_string": {
                "query": f'formType: "13F-HR" AND NOT formType: "13F-HR/A" AND filedAt:[{start_date} TO {end_date}]',
                "time_zone": "UTC+0530" #f"UTC{current_timezone_offset}"
            }
        },
        "from": "0",
        "size": "0",
        "sort": [{ "filedAt": { "order": "desc" } }]
    }

    response = query_api.get_filings(query=query)
    return response['filings']
    # print(response)

# get_data('2023-08-30','2023-08-31')
# create_tables()
@app.task
def run(start,end):
    filings = get_data(start_date=start,end_date=end)
    save_to_db(filings=filings)
    all_unique_cusips = mapping.get_unique_cusip(connection=connection)
# print(all_unique_cusips)
    mapping.fill_holdings_info_preview(all_unique_cusips,connection=connection)

# run('2023-08-30','2023-08-31')