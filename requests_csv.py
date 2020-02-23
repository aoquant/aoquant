import requests
import parsel
import pandas as pd
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
r = requests.get(url='https://www.xicidaili.com/nn',headers=headers)
r.encoding="utf-8"
sel = parsel.Selector(r.text)

# trs = sel.xpath('//table[@id="ip_list"]/tr')
ip = sel.xpath('//table[@id="ip_list"]/tr/td[2]/text()').getall()
port = sel.xpath('//table[@id="ip_list"]/tr/td[3]/text()').getall()
niming = sel.xpath('//table[@id="ip_list"]/tr/td[6]/text()').getall()
livetime = sel.xpath('//table[@id="ip_list"]/tr/td[9]/text()').getall()
certtime = sel.xpath('//table[@id="ip_list"]/tr/td[10]/text()').getall()
# for tr in trs:
#     ip = tr.xpath('.//td[2]/text()').getall()
#     port = tr.xpath('.//td[3]/text()').getall()
#     niming = tr.xpath('.//td[6]/text()').getall()
#     livetime = tr.xpath('.//td[9]/text()').getall()
#     certtime = tr.xpath('.//td[10]/text()').getall()

# print(ip)

data = pd.DataFrame({'ip':ip,'port':port,'匿名':niming,'存活时间':livetime,'认证time':certtime})
data.to_csv("D://requests.csv",index=False,sep=',')

    # if ip and port:
    #     host = ip + port
        #测试代理
        # proxies = {
        #     "http":"http://" + host ,
        #     "https":"https://" + host ,
        # }
        # try:
        #     r = requests.get('https://www.139mt.com',proxies=proxies,timeout=3)
        #     print('可以使用',ip)
        # except requests.exceptions.ProxyError:
        #     print('代理有问题',ip)
        # except requests.exceptions.ConnectTimeout:
        #     print('链接超时',ip)
