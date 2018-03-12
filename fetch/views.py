from django.shortcuts import render

from .database import *
from .constant import *
from .api.managedCustomerLink import *
from .api.campaign import *
from .api.adgroup import *
from .api.keyword import *
from .api.stat import *

import asyncio, datetime

# Route Views
def index(request):
    context = {}
    return render(request, 'report/index.html', context)

# Controll Views
def fetch_accounts():
    db = connect_db('diana')
    users = list(db['users'].find({"type": "naver"}))
    accounts = []
    for user in users:
        if not 'name' in user:
            user['name'] = 'main'
        accounts.append(user)
        params = {'type': 'MYCLIENTS'}
        clients = get_clients_list(user, params)
        for client in clients:
            accounts.append({
                'user_id': user['user_id'],
                'type': 'naver',
                'network_id': str(client['clientCustomerId']),
                'name': client['clientLoginId'],
                'x_api_key': user['x_api_key'],
                'x_secrete': user['x_secrete'],
            })
    nvtest_accounts = db['nvtest_accounts']
    for account in accounts:
        nvtest_accounts.update_one(
            {'network_id': account['network_id']},
            {
                '$set':{
                    'user_id':account['user_id'],
                    'type': 'naver',
                    'name': account['name'],
                    'x_api_key': account['x_api_key'],
                    'x_secrete': account['x_secrete'],
                },
            },
            upsert=True,
        )
    return accounts

def fetch_campaigns(account):
    campaigns = get_campaigns_list(account)
    ids = [campaign['nccCampaignId'] for campaign in campaigns]
    if ids:
        params = {
            'ids': ids,
            'fields': FIELDS['campaign'],
            'datePreset': DATEPRESET,
        }
        stats = get_by_ids(account, params)
        if stats['data']:
            for data in stats['data']:
                data['network_id'] = account['network_id']
                data['dateStart'] = datetime.datetime.now()
                data['dateEnd'] = datetime.datetime.now()
                data['date'] = datetime.datetime.now().strftime('%Y%m%d')
            db = connect_db('diana')
            nvtest_campaigns = db['nvtest_campaigns']
            nvtest_campaigns.insert_many(stats['data'])
            return stats['data']
    return []

def fetch_adgroups(account, campaign):
    params = {'nccCampaignId':campaign['id']}
    adgroups = get_adgroups_list(account, params)
    ids = [adgroup['nccAdgroupId'] for adgroup in adgroups]
    if ids:
        params = {
            'ids': ids,
            'fields': FIELDS['adgroup'],
            'datePreset': DATEPRESET,
        }
        stats = get_by_ids(account, params)
        if stats['data']:
            for data in stats['data']:
                data['network_id'] = account['network_id']
                data['campaign_id'] = campaign['id']
                data['dateStart'] = datetime.datetime.now()
                data['dateEnd'] = datetime.datetime.now()
                data['date'] = datetime.datetime.now().strftime('%Y%m%d')
            db = connect_db('diana')
            nvtest_adgroups = db['nvtest_adgroups']
            nvtest_adgroups.insert_many(stats['data'])
            return stats['data']
    return []

def fetch_keywords(account, adgroup):
    params = {'nccAdgroupId':adgroup['id']}
    keywords = get_keywords_list(account, params)
    ids = [keyword['nccKeywordId'] for keyword in keywords]
    ids_set = [ids[i:i+100] for i in range(0, len(ids), 100)]
    stats_set = []
    for ids in ids_set:
        if ids:
            params = {
                'ids': ids,
                'fields': FIELDS['keyword'],
                'datePreset': DATEPRESET,
            }
            stats = get_by_ids(account, params)
            if stats['data']:
                stats_set += stats['data']
    if stats_set:
        return stats_set
    return []

def fetch_process():
    accounts = fetch_accounts()
    for account in accounts:
        campaigns = fetch_campaigns(account)
        if campaigns:
            for campaign in campaigns:
                adgroups = fetch_adgroups(account, campaign)
                if adgroups:
                    for adgroup in adgroups:
                        keywords = async_fetch_keywords(account, campaign, adgroup)
    return print("fetch_process done!")


def async_fetch_keywords(account, campaign, adgroup):
    params = {'nccAdgroupId':adgroup['id']}
    keywords = get_keywords_list(account, params)
    ids = [keyword['nccKeywordId'] for keyword in keywords]
    ids_set = [ids[i:i+100] for i in range(0, len(ids), 100)]
    params = {
        'fields': FIELDS['keyword'],
        'datePreset': DATEPRESET,
    }
    stats = [async_get_by_ids(account, campaign, adgroup, params, ids) for ids in ids_set]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(stats))
    return stats[0]

async def async_get_by_ids(account, campaign, adgroup, params, ids):
    params['ids'] = ids
    time_now = str(int(round(time.time() * 1000)))
    http_method = "GET"
    request_uri = "/stats"
    headers = {
        'X-Timestamp': time_now,
        'X-API-KEY': account['x_api_key'],
        'X-Customer': account['network_id'],
        'X-Signature': gen_signature(time_now, http_method, request_uri, account['x_secrete']),
    }
    response = requests.get('https://api.naver.com' + request_uri, headers=headers, params=params)
    json_res_data = json.loads(response.text)
    if json_res_data['data']:
        for data in json_res_data['data']:
            data['network_id'] = account['network_id']
            data['campaign_id'] = campaign['id']
            data['adgroup_id'] = adgroup['id']
            data['dateStart'] = datetime.datetime.now()
            data['dateEnd'] = datetime.datetime.now()
            data['date'] = datetime.datetime.now().strftime('%Y%m%d')
        db = connect_db('diana')
        nvtest_keywords = db['nvtest_keywords']
        # nvtest_keywords.insert_many(json_res_data['data'])
        for data in json_res_data['data']:
            nvtest_keywords.update_one(
                {
                    'id':data['id'],
                    'date':data['date']
                },
                {
                    '$set':{
                        'network_id':data['network_id'],
                        'campaign_id':data['campaign_id'],
                        'adgroup_id':data['adgroup_id'],
                        'dateStart':data['dateStart'],
                        'dateEnd':data['dateEnd'],
                        'date':data['date'],
                        'impCnt':data['impCnt'],
                        'clkCnt':data['clkCnt'],
                        'ctr':data['ctr'],
                        'cpc':data['cpc'],
                        'avgRnk':data['avgRnk'],
                        'ccnt':data['ccnt'],
                        'recentAvgRnk':data['recentAvgRnk'],
                        'recentAvgCpc':data['recentAvgCpc'],
                        'salesAmt':data['salesAmt'],
                    }
                },
                upsert=True
            )
    return json_res_data['data']


