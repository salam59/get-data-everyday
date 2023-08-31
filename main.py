import psycopg2
import sys
from sec_api import QueryApi

#postgres connection
try:
    connection = psycopg2.connect(
        "dbname=new user=myuser password=password host=localhost port=5432"
    )
    print("Connected successfully!")
    # connection.close()
except psycopg2.Error as e:
    print("Error:", e)

#SECAPI setup

queryapi = QueryApi(api_key='your api key')

def create_tables():
    cur = connection.cursor()
    create_table_commands = (
        """
        CREATE TABLE filings(
            filing_id varchar(255) PRIMARY KEY,
            cik int,
            filer_name varchar(255),
            period_of_report date
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


def get_filings(start=0,period="2023-06-30"):
    print(f"Getting next 13F batch starting at {start}, {period}")
    query = {
        "query":{
            "query_string":{
                "query": f'formType: "13F-HR" AND NOT formType: "13F-HR/A" AND periodOfReport: "{period}"'
            }
        },
        "from": start,
        "size": "10",
        "sort": [{"filedAt": {"order":"desc"}}]
    }
    responses = queryapi.get_filings(query)
    print("DONE")
    return responses['filings']

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
                period_of_report
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
            filing['periodOfReport']
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

if __name__ == "__main__":
    start = 0
    period = sys.argv[1]

    while start<1000:
        filings = get_filings(start=start,period=period)
        if not filings:
            break

        save_to_db(filings)
        start = start + 73 #taking 10 filings at a time
    print("done-filling")