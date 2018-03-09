from .modules import gen_signature
import time, requests, json

def get_clients_list(user, params):
    # get customer ids query
    time_now = str(int(round(time.time() * 1000)))
    http_method = "GET"
    request_uri = "/customer-links"
    headers = {
        'X-Timestamp': time_now,
        'X-API-KEY': user['x_api_key'],
        'X-Customer': user['network_id'],
        'X-Signature': gen_signature(time_now, http_method, request_uri, user['x_secrete']),
    }
    response = requests.get('https://api.naver.com' + request_uri, headers=headers, params=params)
    json_res_data = json.loads(response.text)
    # print(json_res_data)
    return json_res_data
