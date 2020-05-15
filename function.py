import requests
from bs4 import BeautifulSoup as soup
import os 
path=os.getcwd()


def getlocs(ur):
    r=requests.get(ur)
    Soup=soup(r.text,'html.parser')
    return Soup.findAll('loc')

def reset():
    locs=getlocs('https://www.ibegin.com/sitemap.xml')
    with open('cache.txt','w') as f:
        for l in locs[:-1]:
            for i in range(50):
                f.write(l.text+'|'+str(i)+'|'+'No\n')
            
def geturl():
    with open('cache.txt','r') as f:
        data=f.readlines()
        for d in data:
            flag=d.split('|')[2]
            if('No' in flag):
                key=d.split('|')[1]
                return d.split('|')[0],key
    return None,None

def getfiledata():
    with open('cache.txt','r') as f:
        data=f.readlines()
        return data

def update():
    data=getfiledata()
    with open('cache.txt','w+') as f:
        fg=1
        for d in data:
            flag=d.split('|')[2]
            KEY=d.split('|')[1]
            if('No' in flag and fg==1):
                flag='yes\n'
                fg=0
            f.write(d.split('|')[0]+'|'+KEY+'|'+flag)

