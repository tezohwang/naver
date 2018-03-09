from .modules import gen_signature
import time, requests, json

def get_keywords_list(account, params):
    time_now = str(int(round(time.time() * 1000)))
    http_method = "GET"
    request_uri = "/ncc/keywords"
    headers = {
        'X-Timestamp': time_now,
        'X-API-KEY': account['x_api_key'],
        'X-Customer': account['network_id'],
        'X-Signature': gen_signature(time_now, http_method, request_uri, account['x_secrete']),
    }
    response = requests.get('https://api.naver.com' + request_uri, headers=headers, params=params)
    json_res_data = json.loads(response.text)
    # print(json_res_data)
    return json_res_data