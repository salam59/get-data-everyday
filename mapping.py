import requests
import psycopg2
import time
try:
    connection = psycopg2.connect(
        "dbname=new user=myuser password=password host=localhost port=5432"
    )
    print("Connected successfully!")
    # connection.close()
except psycopg2.Error as e:
    print("Error:", e)

def cusip_to_query(cusip):
    return {"idType": "ID_CUSIP", "idValue": str(cusip)}


def format_response(response):
    if "data" in response and response['data']:
        match = response['data'][0]
        return match['ticker'],match['name'],match['securityType']
    return "","",""


def get_unique_cusip():
    cursor = connection.cursor()
    cursor.execute("SELECT DISTINCT cusip FROM holdings WHERE cusip NOT Like '000%'")
    distinct_cusips = cursor.fetchall()
    # result = []
    # for cusip in distinct_cusips:
    #     result.append(cusip[0])
    return [cusip[0] for cusip in distinct_cusips]


def cusip_to_ticker(cusips):
    api_endpoint = "https://api.openfigi.com/v3/mapping"
    headers = {"X-OPENFIGI-APIKEY": "your api key"}
    query = [cusip_to_query(cusip) for cusip in cusips]
    # print(query)
    try:
        response = requests.post(api_endpoint, json=query, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP error status codes
        results_mapping_cusip = response.json()
        formatted_mappings = [format_response(mapping) for mapping in results_mapping_cusip]
        combined_cusip_metada = [(cusip_nd_data[0],) + cusip_nd_data[1] for cusip_nd_data in list(zip(cusips,formatted_mappings))]
        # print(combined_cusip_metada)
        return combined_cusip_metada
        # print(response.content)
        # print(response.json())
    except requests.exceptions.RequestException as e:
        print("Request Exception:", e)

def fill_holdings_info(cusips_map):

    cursor = connection.cursor()

    insert_commands = """
        INSERT INTO holding_infos(
            cusip,
            ticker,
            security_name,
            security_type
        ) VALUES(%s,%s,%s,%s)
    """
    

    try:
        cursor.executemany(insert_commands,cusips_map)
        cursor.close()
        connection.commit()
    finally:
        connection.commit()


if __name__ == "__main__":
    all_unique_cusips = get_unique_cusip()
    start = 0
    stop = 100
    cusips_length = len(all_unique_cusips)

    while start<cusips_length:
        cusips_batch = []
        if stop < cusips_length:
            cusips_batch = all_unique_cusips[start:stop]
        else:
            cusips_batch[start:]
        
        cusips_map = cusip_to_ticker(cusips_batch)

        fill_holdings_info(cusips_map)

        start += 100
        stop += 100

        if start>= cusips_length:
            break
        time.sleep(5)
        print("----")
    print("DONE")