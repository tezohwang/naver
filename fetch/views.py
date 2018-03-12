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
    update_accounts(accounts)
    print("fetch_accounts done - {}".format(datetime.datetime.now()))
    return accounts

def update_accounts(accounts):
    db = connect_db('diana')
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
        print("update_accounts done - {}".format(datetime.datetime.now()))
    return accounts

def fetch_campaigns(account):
    campaigns = get_campaigns_list(account)
    # print(campaigns)
    ids = [campaign['nccCampaignId'] for campaign in campaigns]
    if ids:
        params = {
            'ids': ids,
            'fields': FIELDS['campaign'],
            'datePreset': DATEPRESET,
        }
        stats = get_by_ids(account, params)
        print("get_by_ids(account) done - {}".format(datetime.datetime.now()))
        if stats['data']:
            for data in stats['data']:
                data['user_id'] = account['user_id']
                data['network_id'] = account['network_id']
                data['dateStart'] = datetime.datetime.now()
                data['dateEnd'] = datetime.datetime.now()
                data['date'] = datetime.datetime.now().strftime('%Y%m%d')
            stats['data'] = combine_campaigns(campaigns, stats['data'])
            update_campaigns(stats['data'])
            return stats['data']
    return []

def combine_campaigns(campaigns, data):
    for _data in data:
        for campaign in campaigns:
            if campaign['nccCampaignId'] == _data['id']:
                _data.update(campaign)
    # print(data)
    print("combine_campaigns done - {}".format(datetime.datetime.now()))
    return data

def update_campaigns(campaigns):
    db = connect_db('diana')
    nvtest_campaigns = db['nvtest_campaigns']
    for campaign in campaigns:
        nvtest_campaigns.update_one(
            {
                'id':campaign['id'],
                'date':campaign['date'],
            },
            {
                '$set':{
                    'user_id':campaign['user_id'],
                    'network_id':campaign['network_id'],
                    'dateStart':campaign['dateStart'],
                    'dateEnd':campaign['dateEnd'],
                    'impCnt':campaign['impCnt'],
                    'clkCnt':campaign['clkCnt'],
                    'ctr':campaign['ctr'],
                    'cpc':campaign['cpc'],
                    'ccnt':campaign['ccnt'],
                    'salesAmt':campaign['salesAmt'],
                    'nccCampaignId':campaign['nccCampaignId'],
                    'customerId':campaign['customerId'],
                    'name':campaign['name'],
                    'userLock':campaign['userLock'],
                    'regTm':campaign['regTm'],
                    'editTm':campaign['editTm'],
                    'dailyBudget':campaign['dailyBudget'],
                    'useDailyBudget':campaign['useDailyBudget'],
                    'status':campaign['status'],
                    'statusReason':campaign['statusReason'],
                    'expectCost':campaign['expectCost'],
                    'migType':campaign['migType'],
                }
            },
            upsert=True,
        )
    print("update_campaigns done - {}".format(datetime.datetime.now()))
    return campaigns

def fetch_adgroups(account, campaign):
    params = {'nccCampaignId':campaign['id']}
    adgroups = get_adgroups_list(account, params)
    # print(adgroups)
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
            stats['data'] = combine_adgroups(adgroups, stats['data'])
            update_adgroups(stats['data'])
            print("fetch_adgroups done - {}".format(datetime.datetime.now()))
            return stats['data']
    return []

def combine_adgroups(adgroups, data):
    for _data in data:
        for adgroup in adgroups:
            if adgroup['nccAdgroupId'] == _data['id']:
                _data.update(adgroup)
    # print(data)
    print("combine_adgroups done - {}".format(datetime.datetime.now()))
    return data

def update_adgroups(adgroups):
    db = connect_db('diana')
    nvtest_adgroups = db['nvtest_adgroups']
    for adgroup in adgroups:
        nvtest_adgroups.update_one(
            {
                'id':adgroup['id'],
                'date':adgroup['date']
            },
            {
                '$set':{
                    'network_id':adgroup['network_id'],
                    'campaign_id':adgroup['campaign_id'],
                    'dateStart':adgroup['dateStart'],
                    'dateEnd':adgroup['dateEnd'],
                    'impCnt':adgroup['impCnt'],
                    'clkCnt':adgroup['clkCnt'],
                    'ctr':adgroup['ctr'],
                    'cpc':adgroup['cpc'],
                    'ccnt':adgroup['ccnt'],
                    'salesAmt':adgroup['salesAmt'],
                    'nccAdgroupId':adgroup['nccAdgroupId'],
                    'customerId':adgroup['customerId'],
                    'nccCampaignId':adgroup['nccCampaignId'],
                    'mobileChannelId':adgroup['mobileChannelId'],
                    'pcChannelId':adgroup['pcChannelId'],
                    'bidAmt':adgroup['bidAmt'],
                    'name':adgroup['name'],
                    'userLock':adgroup['userLock'],
                    'useDailyBudget':adgroup['useDailyBudget'],
                    'useKeywordPlus':adgroup['useKeywordPlus'],
                    'keywordPlusWeight':adgroup['keywordPlusWeight'],
                    'contentsNetworkBidAmt':adgroup['contentsNetworkBidAmt'],
                    'useCntsNetworkBidAmt':adgroup['useCntsNetworkBidAmt'],
                    'mobileNetworkBidWeight':adgroup['mobileNetworkBidWeight'],
                    'pcNetworkBidWeight':adgroup['pcNetworkBidWeight'],
                    'dailyBudget':adgroup['dailyBudget'],
                    'budgetLock':adgroup['budgetLock'],
                    'regTm':adgroup['regTm'],
                    'editTm':adgroup['editTm'],
                    'targetSummary':adgroup['targetSummary'],
                    'pcChannelKey':adgroup['pcChannelKey'],
                    'status':adgroup['status'],
                    'statusReason':adgroup['statusReason'],
                    'expectCost':adgroup['expectCost'],
                    'migType':adgroup['migType'],
                }
            },
            upsert=True,
        )
    print("update_adgroups done - {}".format(datetime.datetime.now()))
    return adgroups

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
    # print(keywords)
    ids = [keyword['nccKeywordId'] for keyword in keywords]
    ids_set = [ids[i:i+100] for i in range(0, len(ids), 100)]
    params = {
        'fields': FIELDS['keyword'],
        'datePreset': DATEPRESET,
    }
    stats = [async_get_by_ids(account, campaign, adgroup, keywords, params, ids) for ids in ids_set]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(stats))
    print("async_fetch_keywords done - {}".format(datetime.datetime.now()))
    return stats[0]

async def async_get_by_ids(account, campaign, adgroup, keywords, params, ids):
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
        json_res_data['data'] = combine_keywords(keywords, json_res_data['data'])
        await update_keywords(json_res_data['data'])
    print("async_get_by_ids done - {}".format(datetime.datetime.now()))
    return json_res_data['data']

def combine_keywords(keywords, data):
    for _data in data:
        for keyword in keywords:
            if keyword['nccKeywordId'] == _data['id']:
                _data.update(keyword)
    # print(data)
    print("combine_keywords done - {}".format(datetime.datetime.now()))
    return data


async def update_keywords(keywords):
    db = connect_db('diana')
    nvtest_keywords = db['nvtest_keywords']
    # nvtest_keywords.insert_many(json_res_data['data'])
    for data in keywords:
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
                    'nccKeywordId':data['nccKeywordId'],
                    'keyword':data['keyword'],
                    'customerId':data['customerId'],
                    'nccAdgroupId':data['nccAdgroupId'],
                    'nccCampaignId':data['nccCampaignId'],
                    'userLock':data['userLock'],
                    'inspectStatus':data['inspectStatus'],
                    'bidAmt':data['bidAmt'],
                    'useGroupBidAmt':data['useGroupBidAmt'],
                    'regTm':data['regTm'],
                    'editTm':data['editTm'],
                    'status':data['status'],
                    'statusReason':data['statusReason'],
                    'nccQi':data['nccQi'],
                }
            },
            upsert=True
        )
    print("update_keywords done - {}".format(datetime.datetime.now()))
    return keywords
