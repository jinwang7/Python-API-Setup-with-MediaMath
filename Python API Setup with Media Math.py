'''
Created on Mon 10/13/20 11:46 am

@auther Jin
version v1.0


'''

# Import necessary packages
import json
import http.client
import pandas as pd
import pyodbc
import datetime
import schedule
import time
import os
import subprocess
import datetime


# Set up routine tasks running everyday

def job():

        # Report timeframe
        date_0 = datetime.datetime.now() - datetime.timedelta(days=1)
        date_ = date_0.strftime('%Y-%m-%d')
        date_compact = date_.replace('-', '')

        date_1 = datetime.datetime.now() - datetime.timedelta(days=1)
        date1_ = date_1.strftime('%Y-%m-%d')
        date_compact1 = date1_.replace('-', '')

        # Get access token - Need to fill out your username, password, client ID, and client secret.
        conn = http.client.HTTPSConnection("auth.mediamath.com")
        payload = "{\"grant_type\":\"password\",\"username\":\"Myusername\",\"password\":\"Mypassword\",\"audience\":\"https://api.mediamath.com/\",\"scope\":\"\",\"client_id\":\"Myclientid\",\"client_secret\":\"Myclientsecret\"}"
        headers = {'content-type': "application/json"}
        conn.request("POST", "/oauth/token", payload, headers)
        res = conn.getresponse()
        data = res.read()

        credential = json.loads(data.decode("utf-8"))

        # Get session ID using token
        conn = http.client.HTTPSConnection("api.mediamath.com")
        conn.request("GET", "/api/v2.0/session", headers={
                     'Accept': "application/vnd.mediamath.v1+json", 'Authorization': "Bearer " + credential['access_token']})
        res = conn.getresponse()
        data = res.read()
        print(data.decode("utf-8"))
        data_id = json.loads(data.decode("utf-8"))
        id = data_id['data']['session']['sessionid']

        # Pull full list of campaign names - Every page has 100 campaigns
        # First 100 pages
        conn = http.client.HTTPSConnection("api.mediamath.com")
        conn.request("GET", "/api/v2.0/campaigns?page_offset=0", headers={
                     'Accept': "application/vnd.mediamath.v1+json", 'Authorization': "Bearer " + credential['access_token']})

        res = conn.getresponse()
        data = res.read()
        camp_id = json.loads(data.decode("utf-8"))


        with open('MMcamp_list.json', 'w') as json_file:
            json.dump(camp_id, json_file)

        with open('MMcamp_list.json') as f:
            list1 = json.load(f)

        # 100 - 200 pages
        conn = http.client.HTTPSConnection("api.mediamath.com")
        conn.request("GET", "/api/v2.0/campaigns?page_offset=100", headers={
            'Accept': "application/vnd.mediamath.v1+json", 'Authorization': "Bearer " + credential['access_token']})

        res = conn.getresponse()
        data = res.read()
        camp_id = json.loads(data.decode("utf-8"))


        with open('MMcamp_list1.json', 'w') as json_file:
            json.dump(camp_id, json_file)

        with open('MMcamp_list1.json') as f:
            list2 = json.load(f)

        # 200 - 300 pages
        conn = http.client.HTTPSConnection("api.mediamath.com")
        conn.request("GET", "/api/v2.0/campaigns?page_offset=200", headers={
            'Accept': "application/vnd.mediamath.v1+json", 'Authorization': "Bearer " + credential['access_token']})

        res = conn.getresponse()
        data = res.read()
        camp_id = json.loads(data.decode("utf-8"))


        with open('MMcamp_list2.json', 'w') as json_file:
            json.dump(camp_id, json_file)

        with open('MMcamp_list2.json') as f:
            list3 = json.load(f)

        if len(list3['data']) == 100:
            # 300 - 400 pages

            conn = http.client.HTTPSConnection("api.mediamath.com")
            conn.request("GET", "/api/v2.0/campaigns?page_offset=300", headers={
                'Accept': "application/vnd.mediamath.v1+json", 'Authorization': "Bearer " + credential['access_token']})

            res = conn.getresponse()
            data = res.read()
            camp_id = json.loads(data.decode("utf-8"))
            # len(camp_id['data'])

            with open('MMcamp_list3.json', 'w') as json_file:
                json.dump(camp_id, json_file)

            with open('MMcamp_list3.json') as f:
                list4 = json.load(f)

        else:
            pass

        camp_list = []
        for i in range(len(list1['data'])):
            camp_list.append(list1['data'][i]['id'])

        for i in range(len(list2['data'])):
            camp_list.append(list2['data'][i]['id'])

        for i in range(len(list3['data'])):
            camp_list.append(list3['data'][i]['id'])

        if 'list4' in locals():
            for i in range(len(list4['data'])):
                camp_list.append(list4['data'][i]['id'])
        else:
            pass

        # Pull report by Campaign
        for item in camp_list:

            conn = http.client.HTTPSConnection("api.mediamath.com")
            headers = {'cookie': "adama_session=%s" % id}
            conn.request(
                "GET", "/reporting/v1/std/geo?precision=8&filter=campaign_id%3D{0}&dimensions=campaign_id%2Ccampaign_name%2Cstrategy_id%2Cstrategy_name%2Ccampaign_start_date%2Ccampaign_timezone_code%2Cregion_name%2Cmetro_name&start_date={1}&end_date={2}&time_rollup=by_day".format(item, date_, date1_), headers=headers)
            res = conn.getresponse()
            data = res.read()
            report = data.decode("utf-8")

            with open('MMgeoReport_' + date_compact + '_' + str(item) + '.csv', 'w', newline='') as f:
                f.write(report)

        # clean the data

        li = []
        for item in camp_list:
            try:
                df = pd.read_csv('MMgeoReport_' +
                                 date_compact + '_' + str(item) + '.csv', nrows=None,  parse_dates=['start_date'])
            except:
                df = pd.DataFrame(columns=['start_date', 'end_date', 'campaign_id', 'campaign_name', 'strategy_id',
                                           'strategy_name', 'campaign_start_date', 'campaign_timezone_code',
                                           'region_name', 'metro_name', 'clicks', 'ctc', 'ctr', 'impressions',
                                           'post_click_conversions', 'post_view_conversions', 'rr_per_1k_imps',
                                           'total_conversions', 'total_spend', 'total_spend_cpa',
                                           'total_spend_cpc', 'total_spend_cpcv', 'total_spend_cpm',
                                           'total_spend_pc_cpa', 'total_spend_pv_cpa', 'video_close',
                                           'video_collapse', 'video_companion_clicks', 'video_companion_ctr',
                                           'video_companion_impressions', 'video_complete', 'video_complete_rate',
                                           'video_complete_rate_impression_based', 'video_engaged_impressions',
                                           'video_engaged_rate', 'video_expand', 'video_first_quartile',
                                           'video_first_quartile_rate', 'video_fullscreen', 'video_midpoint',
                                           'video_midpoint_rate', 'video_mute', 'video_pause', 'video_play_rate',
                                           'video_resume', 'video_rewind', 'video_skip',
                                           'video_skippable_impressions', 'video_skipped_impressions',
                                           'video_skipped_rate', 'video_start', 'video_third_quartile',
                                           'video_third_quartile_rate', 'video_unmute'])

            li.append(df)


            # Next Step - Aggregate the data and push it to local database!
            # Not included


            # Print report
            print("Data has been loaded to DM on " + date_)

# Every morning at 8:00 to fetch data feed
schedule.every().day.at("08:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(60) # wait one minute
