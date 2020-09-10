import scrapy
import json
import re
import datetime
import os
from scrapy.utils.markup import remove_tags
from datetime import date, timedelta
import sys
import logging
from scrapy.utils.log import configure_logging
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.mail import MailSender
from pytz import timezone
import urllib.parse


mailer = MailSender(smtphost='mail.nsi.bg', smtpport=25, mailfrom='nsi-essnet-01@nsi.bg', smtpuser='', smtppass ='')

configure_logging(install_root_handler=False)
logging.basicConfig(
    filename='logs/specific_scraper_'+sys.argv[3].replace('tag=','')+'_'+date.today().strftime('%Y%m%d')+'.log',
    filemode = 'a',
    format='%(asctime)s ----- %(levelname)s ::::: %(message)s ..... (%(name)s)',
    datefmt='%Y-%m-%d %H:%M:%S %Z %z',
    level=logging.DEBUG
)

yesterday='2000-01-01'
root=['_root']
head=''
page_id=1
urllist=[]
priority=0

class specificSpider(scrapy.Spider):
    name = "specific"
    #allowed_domains = ['sanet.st']
    if sys.argv[3].replace('tag=','') in ['booking.com']:
        custom_settings = {
            'USER_AGENT': 'Mozilla/5.0 (Windows NT x.y; Win64; x64; rv:10.0) Gecko/20100101 Firefox/10.0',
        }	
    if sys.argv[3].replace('tag=','') in ['emag.bg.mobilni','emag.bg.tableti']:
        custom_settings = {
            'DOWNLOAD_DELAY': 30,
        }	
    def start_requests(self):
        self.crawler.stats.set_value('start_time', datetime.datetime.now(timezone('Europe/Sofia')).strftime('%Y-%m-%d %H:%M:%S %Z %z'))
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        global yesterday
        global root
        global data
        data=self.get_json(self)
        global head
        for p in data['selectors']:
            if p['type']=='SelectorLink' and p['id']=='pagination':
                head+=p['id']+',priority,'
        for p in data['selectors']:
            if p['type']=='SelectorLink' and p['id']=='page':
                print(p['id'])
                head+=p['id']+','
        for p in data['selectors']:
            if p['id']!='page' and p['id']!='pagination':
                head+=p['id']+','
#            if p['type']=='SelectorText':
#                head+=p['id']+','
#            head+=p['id']+','
        head += 'date_scraped'
#        head = head[:-1]
        head+='\n'
#        print('-------------------------------------------------------------------------------------------------------------')
#        print('startUrl: ' + data['startUrl'][0])
#        print('-------------------------------------------------------------------------------------------------------------')
        custom_settings = {
            'DEPTH_LIMIT': 2
        }
        if sys.argv[3].replace('tag=','') in ['jobs.bg','zaplata.bg']:
            yesterday = datetime.datetime.today() - timedelta(days=1)
        else:
            yesterday = datetime.datetime.today()
        yesterday=yesterday.strftime('%Y%m%d-%H%M%S')
#        page = response.url.split("/")[2]
#        filename = 'data/specific_%s_' % page
        os.makedirs('data/%s' % sys.argv[3].replace('tag=',''), exist_ok=True)
        filename = 'data/%s/specific_scraper_%s_' % (sys.argv[3].replace('tag=',''),sys.argv[3].replace('tag=',''))
        filename+=str(yesterday)+'.csv'
        print('-------------------------------------------------------------------------------------------------------------')
        print('-------------------------------------------------------------------------------------------------------------')
        print('-------------------------------------------------------------------------------------------------------------')
        print(filename)
        print('-------------------------------------------------------------------------------------------------------------')
        print('-------------------------------------------------------------------------------------------------------------')
        print('-------------------------------------------------------------------------------------------------------------')
        with open(filename, 'wb') as f:
            f.write(bytes(head,encoding='utf-8'))
        head=''
        if sys.argv[3].replace('tag=','') in ['booking.com']:
            d1 = date.today() + timedelta(days=1)
            d2 = date.today() + timedelta(days=2)
            yield scrapy.Request(re.sub(r'checkin_year.*group_adults', 'checkin_year='+d1.strftime('%Y')+'&checkin_month='+d1.strftime('%m')+'&checkin_monthday='+d1.strftime('%d')+'&checkout_year='+d2.strftime('%Y')+'&checkout_month='+d2.strftime('%m')+'&checkout_monthday='+d2.strftime('%d')+'&group_adults', data['startUrl'][0]), self.parse)
        elif sys.argv[3].replace('tag=','') in ['bard_top50']:
            print('00000000000000000000000000000000000000000000000000000000000000')
            for d in data['startUrl']:
                yield scrapy.Request(d, self.parse, cookies={'book_list_only_available':'0'})
        elif sys.argv[3].replace('tag=','') in ['address.bg.naemi']:
            print('address.bg.naemi')
            yield scrapy.Request('https://address.bg/services/SearchControlService.aspx?lang=bg&segmentId=2&city=104&etype=4&etype=3&etype=5&etype=15&etype=9&etype=2&etype=12&etype=17&etype=18&pricefrom=&priceto=&areafrom=&areato=&sortBy=1&offerno=&floorfrom=&floorto=', self.parse)
            for d in data['startUrl']:
                yield scrapy.Request(d, self.parse)
        else:
            for d in data['startUrl']:
#                print(urllib.parse.unquote(d))			
                yield scrapy.Request(urllib.parse.unquote(d), self.parse)


    def parse(self, response):
        global yesterday
        global root
        global data
        global head
        global page_id
        global urllist
        global priority
        for p in data['selectors']:
            if p['type']=='SelectorLink' and p['id']=='page':
                urllist.append(response.url)
#                head=str(page_id)+','+response.url+','# s url na stranicata
                head=str(page_id)+','
                page_id=page_id+1
                next_page=response.css(p['selector'].replace('\"','"')+'::attr(href)').getall()
                if isinstance(next_page, str):
                    priority=priority+1
                    yield scrapy.Request(response.urljoin(urllib.parse.unquote(next_page)), callback=self.parse_page2, meta={'head':head,'priority': priority}, priority=priority)
                else:
                    for n in next_page:
                        priority=priority+1
                        n=response.urljoin(urllib.parse.unquote(n))
                        yield scrapy.Request(response.urljoin(urllib.parse.unquote(n)), callback=self.parse_page2, meta={'head':head,'priority': priority}, priority=priority)
            if p['type']=='SelectorElement' and (p['id']=='element' or p['id']=='Елемент'): # za booking.com
                priority=priority+1
                head=str(page_id)+','
                yield scrapy.Request(response.urljoin(response.url+'&1'), callback=self.parse_page3, meta={'head':head,'priority': priority}, priority=priority)
        for p in data['selectors']:
#            print('-------------------------------------------------------------------------------------------------------------')
#            print('type: ' + p['type'] + ' ' + root[0])
#            print('-------------------------------------------------------------------------------------------------------------')
            if any(x in p['parentSelectors'] for x in root) and p['type']=='SelectorLink' and p['id']=='pagination':
                next_page=response.css(p['selector'].replace('\"','"')+'::attr(href)').getall()
                if isinstance(next_page, str):
                    root=[p['id']]#
                    urllist.append(response.urljoin(urllib.parse.unquote(next_page)))
#                    head=str(page_id)+','+response.urljoin(next_page)+','# s url na stranicata
                    head=str(page_id)+','
                    page_id=page_id+1#
                    yield scrapy.Request(response.urljoin(urllib.parse.unquote(next_page)), callback=self.parse_page1, meta={'head':head})
                else:
                    for n in next_page:
                        root=[p['id']]
                        urllist.append(response.urljoin(urllib.parse.unquote(n)))
#                        head=str(page_id)+','+response.urljoin(n)+','# s url na stranicata
                        head=str(page_id)+','
                        page_id=page_id+1
                        yield scrapy.Request(response.urljoin(urllib.parse.unquote(n)), callback=self.parse_page1, meta={'head':head})
            if any(x in p['parentSelectors'] for x in root) and p['type']=='SelectorElement' and (p['id']=='element' or p['id']=='Елемент'): # za booking.com
                priority=priority+1
                root=[p['id']]#
                yield scrapy.Request(response.urljoin(response.url+'&1'), callback=self.parse_page3, meta={'head':head,'priority': priority}, priority=priority)

    def parse_page1(self, response):
        global root
        global data
        global page_id
        global urllist
        global priority
        head=response.meta['head']
        for p in data['selectors']:
#            print('-------------------------------------------------------------------------------------------------------------')
#            print('type: ' + p['type'] + ' ' + root[0])
#            print('-------------------------------------------------------------------------------------------------------------')
            if any(x in p['parentSelectors'] for x in root) and p['type']=='SelectorLink' and p['id']=='page':
                next_page=response.css(p['selector'].replace('\"','"')+'::attr(href)').getall()
                if isinstance(next_page, str):
                    priority=priority+1
                    yield scrapy.Request(response.urljoin(urllib.parse.unquote(next_page)), callback=self.parse_page2, meta={'head':head,'priority': priority}, priority=priority)
                else:
                    for n in next_page:
                        priority=priority+1
                        n=response.urljoin(urllib.parse.unquote(n))
                        yield scrapy.Request(response.urljoin(urllib.parse.unquote(n)), callback=self.parse_page2, meta={'head':head,'priority': priority}, priority=priority)
            if p['type']=='SelectorElement' and p['id']=='element': # za booking.com
                priority=priority+1
                yield scrapy.Request(response.urljoin(response.url+'&1'), callback=self.parse_page3, meta={'head':head,'priority': priority}, priority=priority)
            if p['type']=='SelectorLink' and p['id']=='pagination':
                next_page=response.css(p['selector'].replace('\"','"')+'::attr(href)').getall()
                if isinstance(next_page, str):
                    if response.urljoin(urllib.parse.unquote(next_page)) not in urllist:#
                        urllist.append(response.urljoin(urllib.parse.unquote(next_page)))#
#                        head=str(page_id)+','+response.urljoin(next_page)+','# s url na stranicata
                        head=str(page_id)+','#
                        page_id=page_id+1#
                        yield scrapy.Request(response.urljoin(urllib.parse.unquote(next_page)), callback=self.parse_page1, meta={'head':head})
#                    yield scrapy.Request(response.urljoin(next_page), callback=self.parse_page1, meta={'head':head})
                else:
                    for n in next_page:
                        if response.urljoin(urllib.parse.unquote(n)) not in urllist:
                            urllist.append(response.urljoin(urllib.parse.unquote(n)))
#                            head=str(page_id)+','+response.urljoin(n)+','# s url na stranicata
                            head=str(page_id)+','
                            page_id=page_id+1
                            yield scrapy.Request(response.urljoin(urllib.parse.unquote(n)), callback=self.parse_page1, meta={'head':head})


    def parse_page2(self, response):
        global yesterday
        global root
        global data
        head=response.meta['head']
        priority=response.meta['priority']
        head+='"'+str(priority)+'",'
        head+='"'+response.url+'",'
        for p in data['selectors']:
#            print('************************************************************************************************************************************')
#            print(response.meta['head'])
#            print('************************************************************************************************************************************')
            if p['type']=='SelectorText':
#                print('##############################################################################################################################')
#                print(p['selector'])
#                print('##############################################################################################################################')
                s=response.css(p['selector']).get()
                if isinstance(s, str) and s is not None and s is not '':
                    if isinstance(p['regex'], str) and p['regex'] is not None and p['regex'] is not '':
                        s=re.sub(r'<(script).*?</\1>(?s)', ' ', s)
                        s=re.sub(r'<(style).*?</\1>(?s)', ' ', s)
                        s=remove_tags(s)
                        s=s.replace('"','""')
                        s=s.replace('\n','')
                        s=s.replace('\r','')
                        s=s.replace('\t',' ')
                        s=re.sub(r'\s+', ' ', s).strip()
                        print('#############################################################')
                        print(p['regex'])
                        print(s)
                        print('#############################################################')
                        g=re.search(p['regex'],s)
                        if g:
                            print('>>>>>>>>>>')
                            print(g.group())
                            print('>>>>>>>>>>')
                            s=g.group()
                        else:
                            s=''
                        s=s.strip()
                        head+='"'+s+'",'
                    else:
                        s=re.sub(r'<(script).*?</\1>(?s)', ' ', s)
                        s=re.sub(r'<(style).*?</\1>(?s)', ' ', s)
                        s=remove_tags(s)
                        s=s.replace('"','""')
                        s=s.replace('\n','')
                        s=s.replace('\r','')
                        s=s.replace('\t',' ')
                        s=re.sub(r'\s+', ' ', s).strip()
                        head+='"'+s+'",'
                else:
                    head+='"",'
            if p['type']=='SelectorElementAttribute':
#                print('##############################################################################################################################')
#                print(p['selector'])
#                print('##############################################################################################################################')
                if response.css(p['selector']):
                    s=response.css(p['selector']).attrib[p['extractAttribute']]
                    if isinstance(s, str) and s is not None and s is not '':
                        s=re.sub(r'<(script).*?</\1>(?s)', ' ', s)
                        s=re.sub(r'<(style).*?</\1>(?s)', ' ', s)
                        s=remove_tags(s)
                        s=s.replace('"','""')
                        s=s.replace('\n','')
                        s=s.replace('\r','')
                        s=s.replace('\t',' ')
                        s=re.sub(r'\s+', ' ', s).strip()
                        head+='"'+s+'",'
                        print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                        print(s)
                        print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                    else:
                        head+='"",'
                else:
                    head+='"",'
#        print('nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn')
#        print(response.url)
#        print('nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn')
        head += '"'+datetime.datetime.now(timezone('Europe/Sofia')).strftime('%Y-%m-%d %H:%M:%S %Z %z')+'"'
#        head = head[:-1]
        head+='\n'

#        page = response.url.split("/")[2]
#        filename = 'data/specific_%s_' % page
        filename = 'data/%s/specific_scraper_%s_' % (sys.argv[3].replace('tag=',''),sys.argv[3].replace('tag=',''))
        filename+=str(yesterday)+'.csv'
        with open(filename, 'ab') as f:
            f.write(bytes(head,encoding='utf-8'))
        head=''


    def get_json(self, data):
        tag = getattr(self, 'tag', None)
        if tag is not None:
            with open('sites/'+tag+'.json', encoding='utf-8') as json_file:  
                data = json.load(json_file)
                print(json.dumps(data, indent=4))
        return data

    def spider_closed(self):
        global yesterday
#        mailer.send(to=["kgeorgiev@nsi.bg"], subject="Some subject", body="Some body"+str(self.crawler.stats.get_stats()))
        self.crawler.stats.set_value('finish_time', datetime.datetime.now(timezone('Europe/Sofia')).strftime('%Y-%m-%d %H:%M:%S %Z %z'))
        res = '\r\n\r\n'.join(['%s ::: %s' % (key, value) for (key, value) in self.crawler.stats.get_stats().items()])
        tag=sys.argv[3].replace('tag=','')
        if tag=='pochivka.bg' or tag=='booking.com':
            mailer.send(to=["kgeorgiev@nsi.bg","avukov@nsi.bg","tstefanova@nsi.bg"], subject="Scrape info ... "+tag+" ::: "+yesterday, body=res)
#            mailer.send(to=["kgeorgiev@nsi.bg"], subject="Scrape info ... "+tag+" ::: "+yesterday, body=res)
        elif tag=='jobs.bg' or tag=="zaplata.bg":
            mailer.send(to=["kgeorgiev@nsi.bg","nveleva@nsi.bg","pshtarbanov@nsi.bg"], subject="Scrape info ... "+tag+" ::: "+yesterday, body=res)
        elif tag=='ozon.bg.muzika' or tag=='olx.bg.naemi.ek' or tag=='olx.bg.naemi' or tag=='imot.bg.naemi' or tag=='helikon_music' or tag=='obsidian.bg' or tag=='pulsar_nai-prodavani' or tag=='booktrading_books_top' or tag=='bard_top50' or tag=='bard_bestsellers' or tag=='helikon_top10_teen' or tag=='helikon_top10_nehudojestvena' or tag=='helikon_top10_hudojestvena':
            mailer.send(to=["kgeorgiev@nsi.bg","nveleva@nsi.bg"], subject="Scrape info ... "+tag+" ::: "+yesterday, body=res)
        elif  tag=='imoti.com.naemi' or tag=='nedvijim.com.naemi' or tag=='homes.bg.naemi.ek' or tag=='homes.bg.naemi' or tag=='alo.bg.naemi' or tag=='uchebnika.bg.pomagala' or tag=='uchebnika.bg.uchebnici' or tag=='e-uchebnik.bg.7.uchebni.pomagala' or tag=='e-uchebnik.bg.9.uchebnici' or tag=='unipress.bg' or tag=='uchebnika.bg.rechnici' or tag=='sms.bg.parts' or tag=='plesio.bg.tableti' or tag=='plesio.bg.mobilni' or tag=='plesio.bg.parts' or tag=='smartphone.bg.mobilni' or tag=='pcstore.bg.mobilni' or tag=='pcstore.bg.tableti' or tag=='emag.bg.tableti' or tag=='emag.bg.mobilni' or tag=='technopolis.bg.mobilni' or tag=='amco-shop.com.tableti' or tag=='computer-store.bg.tableti' or tag=='speedcomputers.biz.tableti' or tag=='technomarket.bg.tableti' or tag=='tablet.bg.tableti' or tag=='technopolis.bg.tableti' or tag=='jarcomputers.com' or tag=='jarcomputers.com.tableti' or tag=='jarcomputers.com.mobilni' or tag=='most.bg':
            mailer.send(to=["kgeorgiev@nsi.bg","pshtarbanov@nsi.bg"], subject="Scrape info ... "+tag+" ::: "+yesterday, body=res)
        elif tag=='mobile.bg' or tag=='amco-shop.com.mobilni' or tag=='ardes.bg.mobilni' or tag=='datacom.bg.mobilni' or tag=='datacom.bg.mobilni.EN.Prestigio' or tag=='address.bg.naemi' or tag=='mirela.bg.naemi' or tag=='holmes.bg.naemi':
            mailer.send(to=["kgeorgiev@nsi.bg","vkostadinova@nsi.bg"], subject="Scrape info ... "+tag+" ::: "+yesterday, body=res)
        else:
            mailer.send(to=["kgeorgiev@nsi.bg"], subject="Scrape info ... "+tag+" ::: "+yesterday, body=res)



    def parse_page3(self, response): # za booking.com
        global yesterday
        global root
        global data
        head=''
#        head=response.meta['head']
        for pp in data['selectors']:
#            print('************************************************************************************************************************************')
#            print(response.meta['head'])
#            print('************************************************************************************************************************************')
            if pp['type']=='SelectorElement' and (pp['id']=='element' or pp['id']=='Елемент'): # za booking.com
                element=response.css(pp['selector']).getall()
                i=0
                for e in element:
                    head+=response.meta['head']
                    priority=response.meta['priority']
                    head+='"'+str(priority)+'",'
                    head+='"'+response.url.split("//")[-1].split("/")[0].split('?')[0]+'",'
                    for p in data['selectors']:
                        if p['type']=='SelectorText':
#                            print('##############################################################################################################################')
#                            print(p['selector'])
#                            print('##############################################################################################################################')
                            s=response.css(p['selector']).getall()
                            if i<len(s):
                                if isinstance(s[i], str) and s[i] is not None and s[i] is not '':
                                    if isinstance(p['regex'], str) and p['regex'] is not None and p['regex'] is not '':
                                        s=re.sub(r'<(script).*?</\1>(?s)', ' ', s[i])
                                        s=re.sub(r'<(style).*?</\1>(?s)', ' ', s)
                                        s=remove_tags(s)
                                        s=s.replace('"','""')
                                        s=s.replace('\n','')
                                        s=s.replace('\r','')
                                        s=s.replace('\t',' ')
                                        s=re.sub(r'\s+', ' ', s).strip()
#                                        print('#############################################################')
#                                        print(p['regex'])
#                                        print(s)
#                                        print('#############################################################')
                                        g=re.search(p['regex'],s)
                                        if g:
#                                            print('>>>>>>>>>>')
#                                            print(g.group())
#                                            print('>>>>>>>>>>')
                                            s=g.group()
                                        else:
                                            s=''
                                        s=s.strip()
                                        head+='"'+s+'",'
                                    else:
                                        s=re.sub(r'<(script).*?</\1>(?s)', ' ', s[i])
                                        s=re.sub(r'<(style).*?</\1>(?s)', ' ', s)
                                        s=remove_tags(s)
                                        s=s.replace('"','""')
                                        s=s.replace('\n','')
                                        s=s.replace('\r','')
                                        s=s.replace('\t',' ')
                                        s=re.sub(r'\s+', ' ', s).strip()
                                        head+='"'+s+'",'
                                else:
                                    head+='"-",'
                        if p['type']=='SelectorElementAttribute':
#                print('##############################################################################################################################')
#                print(p['selector'])
#                print('##############################################################################################################################')
                            if response.css(pp['selector']+' '+p['selector']):
                                s=response.css(pp['selector']+' '+p['selector']).attrib[p['extractAttribute']]
                                if isinstance(s, str) and s is not None and s is not '':
                                    s=re.sub(r'<(script).*?</\1>(?s)', ' ', s)
                                    s=re.sub(r'<(style).*?</\1>(?s)', ' ', s)
                                    s=remove_tags(s)
                                    s=s.replace('"','""')
                                    s=s.replace('\n','')
                                    s=s.replace('\r','')
                                    s=s.replace('\t',' ')
                                    s=re.sub(r'\s+', ' ', s).strip()
                                    head+='"'+s+'",'
                                    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                                    print(s)
                                    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                                else:
                                    head+='"",'
                            else:
                                head+='"",'
                    i=i+1
#        print('nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn')
#        print(response.url)
#        print('nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn')
                    head += '"'+datetime.datetime.now(timezone('Europe/Sofia')).strftime('%Y-%m-%d %H:%M:%S %Z %z')+'"'
#        head = head[:-1]
                    head+='\n'

#        page = response.url.split("/")[2]
#        filename = 'data/specific_%s_' % page
        filename = 'data/%s/specific_scraper_%s_' % (sys.argv[3].replace('tag=',''),sys.argv[3].replace('tag=',''))
        filename+=str(yesterday)+'.csv'
        with open(filename, 'ab') as f:
            f.write(bytes(head,encoding='utf-8'))
        head=''

		
		




