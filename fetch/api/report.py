from .modules import gen_signature
import time, requests, json

def get_reports_list(account):
    time_now = str(int(round(time.time() * 1000)))
    http_method = "GET"
    request_uri = "/stat-reports"
    params = {
        'reportJobId':'326576716',
    }
    headers = {
        'X-Timestamp': time_now,
        'X-API-KEY': account['x_api_key'],
        'X-Customer': account['network_id'],
        'X-Signature': gen_signature(time_now, http_method, request_uri, account['x_secrete']),
    }
    response = requests.get('https://api.naver.com' + request_uri, headers=headers, params=params)
    # json_res_data = json.loads(response.text)
    print(response.text)
    return response
