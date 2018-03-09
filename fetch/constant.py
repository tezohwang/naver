# 앱에 쓰이는 상수 선언

DATABASE = {
    'diana': {
        'db_uri': 'mongodb://wizpace:wizpace0@52.78.216.222:27017/diana',
        'db_name': 'diana'
    },
    'autobidding':{
        'db_uri': 'mongodb://autobidding:autobid0@13.125.87.196:27017/autobidding',
        'db_name': 'autobidding'
    }
}

FIELDS = {
    'campaign': '["impCnt","clkCnt","ctr","cpc","ccnt","salesAmt"]',
    'adgroup': '["impCnt","clkCnt","ctr","cpc","ccnt","salesAmt"]',
    'keyword': '["impCnt","clkCnt","ctr","cpc","avgRnk","ccnt","recentAvgRnk","recentAvgCpc","salesAmt"]',
}

# today / yesterday / last7days / last30days / lastweek / lastmonth / lastquarter
DATEPRESET = 'today'

TIME = {
    'sleep_time': 0.01
}
