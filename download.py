import os
from aiohttp import ClientSession
import asyncio
from bs4 import BeautifulSoup as soup
import csv
import requests
import time
from function import geturl,getlocs,update
import json


async def extract(url, session):
    url='http://www.ibegin.com'+url
    retries=0
    while(retries!=5):
        try:
            async with session.get(url) as response:
                delay = response.headers.get("DELAY")
                date = response.headers.get("DATE")
                print("{}:{} with delay {}".format(date, response.url, delay))
                content= await response.read()
                Soup=soup(content,'html.parser')
                try:
                    name=Soup.find('dd',attrs={'class':'fn org name'}).text.strip()
                except:
                    name=''
                try:
                    add1=Soup.find('dd',attrs={'class':'street-address address'}).text.strip()
                except:
                    try:
                        add1=Soup.find('dd',attrs={'class':'street-address address.address'}).text.strip()
                    except:
                        add1=''
                try:
                    city=Soup.find('dd',attrs={'class':'address.city'}).text.strip()
                except:
                    city=''
                try:
                    pro=Soup.find('dd',attrs={'class':'address.state'}).text.strip()
                except:
                    pro=''
                add=add1+' '+city+' '+pro
                add=add.replace(',',';').strip()
                try:
                    phn=Soup.find('abbr',attrs={'class':'tel phone'}).get('title')
                except:
                    phn=''
                try:
                    web=Soup.find('dd',attrs={'class':'url'}).find('a').get('href')
                except:
                    web=''
                
                ans=[name,add,phn,web]
                print(ans)
                return ans
        except:
            print("Retrying "+url)
            retries=retries+1
    return []


async def fetch(url, session):
    retries=0
    while(retries!=5):
        try:
            async with session.get(url) as response:
                delay = response.headers.get("DELAY")
                date = response.headers.get("DATE")
                print("{}:{} with delay {}".format(date, response.url, delay))
                content= await response.read()
                Soup=soup(content,'html.parser')
                buiss=Soup.findAll('div',attrs={'class':'business'})
                ans=[]
                for buis in buiss:
                    a=buis.find('a')
                    if(a):
                        row=await extract(a.get('href'),session)
                        ans.append(row)
                return ans
        except:
            print("Retrying "+url)
            retries=retries+1
    return []



async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(url, session)

async def run(r,locs,seed):
    tasks = []
    # create instance of Semaphore
    sem = asyncio.Semaphore(seed)
    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        for i in range(r):
            # pass Semaphore and session to every GET request
            task = asyncio.ensure_future(bound_fetch(sem, locs[i].text, session))
            tasks.append(task)
        return await  asyncio.gather(*tasks)


def download(file,seed):
    url,key=geturl()
    if(url==None):
        return None
    print(url+' Part-'+key)
    data=[]
    locs=getlocs(url)
    for l in locs:
        if('/category/' in l.text):
            data.append(l)
    key=int(key)
    try:
        locs=data[key*1000:(key+1)*1000]
    except:
        update()
        return url
    number=len(locs)
    print(number)
    try:
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(number,locs,seed))
        rows=loop.run_until_complete(future)
    except:
        loop.stop()
        print("retrying sitemap "+url)
        return url
    dd=[]
    with open(file,'a') as f:
        for row in rows:
            if(row==[]):
                continue
            for r in row:
                if(r==[]):
                    continue
                try:
                    if(r[0]==''):
                        continue
                except:
                    continue
                if(r[2] in dd):
                    continue
                try:
                    f.write(r[0]+','+r[1]+','+r[2]+','+r[3]+'\n')
                except:
                    continue
                dd.append(r[2])
    update()
    return url

