name='oja'
where='\\\\path-to\\\python\\scrapy\\'
path=r'{0}data\\oja\\'.format(where)
where='..\\\\'
path=r'{0}data\\oja\\'.format(where)
cpath='\\\\path-to-oja\\\\web\\\directory\\'
cpath='D:\\\\path-to-oja\\\\web\\\directory\\'
web='/oja/'
server='mail-server-address'
port='mail-server-port'
emailfrom = "PythonJupyter@nsi.bg"
emailto = ["email1@example.com","email2@example.com","email3@example.com","email4@example.com","email5@example.com"]
#emailto = ["email2@example.com"]
#emailto = ["email1@example.com"]

import pandas as pd
import numpy as np
import io

import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

#import numpy as np

df = pd.read_csv(r'{0}stat\\{1}.encode.files.bg.csv'.format(path,name),delimiter=",",encoding = "utf-8")
df=df.rename(columns={"NUTS3_NAME_BG": "NUTS 3 BG", "NUTS3_NAME_LAT": "NUTS 3", "obrazovanie": "Obrazovanie", "obrazovanie_en": "Educational level", "rabotno_vreme_en": "Full and part time work", "KID1_08": "NACE Level 1"})
df.shape

str = io.StringIO()
str.write('''<html>
    <head>
        <title>OJA indicators by quarters</title>
        <link type="text/css" rel="stylesheet" href="oja.css" media="all" />
    </head>
    <body>
        <h1>OJA indicators by quarters</h1>
        <div>
        <ul>Download data:
        <li>
        <a href="OJA_by_quarters.csv" title="Download OJA by quarters in CSV">OJA_by_quarters.csv</a>
        </li>
        <li>
        <a href="OJA_by_quarters.xlsx" title="Download OJA by quarters in XLSX">OJA_by_quarters.xlsx</a>
        </li>
        <li>
        <a href="OJA_by_quarters_Educational_level.csv" title="Download OJA by quarters Educational level in CSV">OJA_by_quarters_Educational_level.csv</a>
        </li>
        <li>
        <a href="OJA_by_quarters_Educational_level.xlsx" title="Download OJA by quarters Educational level in XLSX">OJA_by_quarters_Educational_level.xlsx</a>
        </li>
        <li>
        <a href="OJA_by_quarters_Full_and_part_time_work.csv" title="Download OJA by quarters Full and part time work in CSV">OJA_by_quarters_Full_and_part_time_work.csv</a>
        </li>
        <li>
        <a href="OJA_by_quarters_Full_and_part_time_work.xlsx" title="Download OJA by quarters Full and part time work in XLSX">OJA_by_quarters_Full_and_part_time_work.xlsx</a>
        </li>
        <li>
        <a href="OJA_by_quarters_NACE_Level_1.csv" title="Download OJA by quarters NACE Level 1 in CSV">OJA_by_quarters_NACE_Level_1.csv</a>
        </li>
        <li>
        <a href="OJA_by_quarters_NACE_Level_1.xlsx" title="Download OJA by quarters NACE Level 1 in XLSX">OJA_by_quarters_NACE_Level_1.xlsx</a>
        </li>
        </ul>
        </div>
''')

dfw=df
dfw['Quarter'] = pd.to_datetime(dfw['data'],dayfirst='true').dt.to_period('Q')
dfw = dfw.groupby(['Quarter'], as_index=False)['rabota'].count()
dfw.rename(columns={"rabota": "Number"},inplace=True)
dfw['Change'] = dfw['Number'].pct_change()
dfw=dfw.fillna(0)
dfw.to_csv(r'{0}OJA_by_quarters.csv'.format(cpath), index = None, header=True)
dfw.to_excel(r'{0}OJA_by_quarters.xlsx'.format(cpath), sheet_name='Sheet1',encoding="utf-8-sig" , index = None, header=True)
#dfw['Period'] = dfw['Quarter'].apply(lambda x: r'from {0} to {1}'.format(str(x).split('/')[0],str(x).split('/')[1]))
dfw

ax1=dfw.plot(x='Quarter', y='Number',figsize=(10, 5),rot=45,lw=2,title='OJA by quarters Number',grid=True)
#ax.set(xlabel="x label", ylabel="y label")
fig = ax1.get_figure()
fig.savefig(r'{0}OJA_by_quarters_Number.png'.format(cpath))

ax2=dfw.plot(x='Quarter', y='Change',figsize=(10, 5),rot=45,lw=2,title='OJA by quarters Change',grid=True)
fig = ax2.get_figure()
fig.savefig(r'{0}OJA_by_quarters_Change.png'.format(cpath))

str.write('<h2>OJA by quarters</h2>')
dfw.to_html(buf=str, classes='table table-oja')
str.write('''
        <div>
        <img src="OJA_by_quarters_Number.png" alt="Line chart of OJA by quarters Number" title="OJA by quarters Number"/>
        <img src="OJA_by_quarters_Change.png" alt="Line chart of OJA by quarters Change" title="OJA by quarters Change"/>
        </div>
''')

dfw=df
dfw['Quarter'] = pd.to_datetime(dfw['data'],dayfirst='true').dt.to_period('Q')
dfw1 = dfw.groupby(['Quarter','Educational level'], as_index=False)['rabota'].count()
dfwp=dfw1.pivot(index='Quarter', columns='Educational level', values='rabota')
dfwp.rename(columns={"rabota": "Number"},inplace=True)
dfwp['Higher Change'] = dfwp['Higher'].pct_change()
dfwp['Primary Change'] = dfwp['Primary'].pct_change()
dfwp['Secondary Change'] = dfwp['Secondary'].pct_change()
dfwp = dfwp.reset_index()
dfwp=dfwp.fillna(0)
dfwp.to_csv(r'{0}OJA_by_quarters_Educational_level.csv'.format(cpath), index = None, header=True)
dfwp.to_excel(r'{0}OJA_by_quarters_Educational_level.xlsx'.format(cpath), sheet_name='Sheet1',encoding="utf-8-sig" , index = None, header=True)
#dfw['Period'] = dfw['Quarter'].apply(lambda x: r'from {0} to {1}'.format(str(x).split('/')[0],str(x).split('/')[1]))
dfwp

ax1=dfwp.plot(x='Quarter', y={'Higher','Primary','Secondary'},figsize=(10, 5),rot=45,lw=2,title='OJA by quarters Educational level Number',grid=True)
#ax.set(xlabel="x label", ylabel="y label")
fig = ax1.get_figure()
fig.savefig(r'{0}OJA_by_quarters_Educational_level_Number.png'.format(cpath))

ax2=dfwp.plot(x='Quarter', y={'Higher Change','Primary Change','Secondary Change'},figsize=(10, 5),rot=45,lw=2,title='OJA by quarters Educational level Change',grid=True)
fig = ax2.get_figure()
fig.savefig(r'{0}OJA_by_quarters_Educational_level_Change.png'.format(cpath))

str.write('<h2>OJA by quarters Educational level</h2>')
dfwp.to_html(buf=str, classes='table table-oja')
str.write('''
        <div>
        <img src="OJA_by_quarters_Educational_level_Number.png" alt="Line chart of OJA by quarters Educational level Number" title="OJA by quarters Educational level Number"/>
        <img src="OJA_by_quarters_Educational_level_Change.png" alt="Line chart of OJA by quarters Educational level Change" title="OJA by quarters Educational level Change"/>
        </div>
''')

dfw=df
dfw['Quarter'] = pd.to_datetime(dfw['data'],dayfirst='true').dt.to_period('Q')
dfw1 = dfw.groupby(['Quarter','Full and part time work'], as_index=False)['rabota'].count()
dfwp=dfw1.pivot(index='Quarter', columns='Full and part time work', values='rabota')
dfwp.rename(columns={"rabota": "Number"},inplace=True)
dfwp['Full time work Change'] = dfwp['Full time work'].pct_change()
dfwp['Part-time work Change'] = dfwp['Part-time work'].pct_change()
dfwp = dfwp.reset_index()
dfwp=dfwp.fillna(0)
dfwp.to_csv(r'{0}OJA_by_quarters_Full_and_part_time_work.csv'.format(cpath), index = None, header=True)
dfwp.to_excel(r'{0}OJA_by_quarters_Full_and_part_time_work.xlsx'.format(cpath), sheet_name='Sheet1',encoding="utf-8-sig" , index = None, header=True)
#dfw['Period'] = dfw['Quarter'].apply(lambda x: r'from {0} to {1}'.format(str(x).split('/')[0],str(x).split('/')[1]))
dfwp

ax1=dfwp.plot(x='Quarter', y={'Full time work','Part-time work'},figsize=(10, 5),rot=45,lw=2,title='OJA by quarters Full and part time work Number',grid=True)
fig = ax1.get_figure()
fig.savefig(r'{0}OJA_by_quarters_Full_and_part_time_work_Number.png'.format(cpath))

ax2=dfwp.plot(x='Quarter', y={'Full time work Change','Part-time work Change'},figsize=(10, 5),rot=45,lw=2,title='OJA by quarters Full and part time work Change',grid=True)
fig = ax2.get_figure()
fig.savefig(r'{0}OJA_by_quarters_Full_and_part_time_work_Change.png'.format(cpath))

str.write('<h2>OJA by quarters Full and part time work</h2>')
dfwp.to_html(buf=str, classes='table table-oja')
str.write('''
        <div>
        <img src="OJA_by_quarters_Full_and_part_time_work_Number.png" alt="Line chart of OJA by quarters Full and part time work Number" title="OJA by quarters Full and part time work Number"/>
        <img src="OJA_by_quarters_Full_and_part_time_work_Change.png" alt="Line chart of OJA by quarters Full and part time work Change" title="OJA by quarters Full and part time work Change"/>
        </div>
''')

dfw=df
dfw['Quarter'] = pd.to_datetime(dfw['data'],dayfirst='true').dt.to_period('Q')
dfw1 = dfw.groupby(['Quarter','NACE Level 1'], as_index=False)['rabota'].count()
dfwp=dfw1.pivot(index='Quarter', columns='NACE Level 1', values='rabota')
dfwp.rename(columns={"rabota": "Number"},inplace=True)
dfwp=dfwp.fillna(0)
dfwp['C Change'] = dfwp['C'].pct_change()
dfwp['D Change'] = dfwp['D'].pct_change()
dfwp['E Change'] = dfwp['E'].pct_change()
dfwp['F Change'] = dfwp['F'].pct_change()
dfwp['G Change'] = dfwp['G'].pct_change()
dfwp['H Change'] = dfwp['H'].pct_change()
dfwp['I Change'] = dfwp['I'].pct_change()
dfwp['J Change'] = dfwp['J'].pct_change()
dfwp['L Change'] = dfwp['L'].pct_change()
dfwp['M Change'] = dfwp['M'].pct_change()
dfwp['N Change'] = dfwp['N'].pct_change()
dfwp['S Change'] = dfwp['S'].pct_change()
dfwp = dfwp.reset_index()
dfwp=dfwp.replace([np.inf, -np.inf], np.nan)
dfwp=dfwp.fillna(0)
dfwp.to_csv(r'{0}OJA_by_quarters_NACE_Level_1.csv'.format(cpath), index = None, header=True)
dfwp.to_excel(r'{0}OJA_by_quarters_NACE_Level_1.xlsx'.format(cpath), sheet_name='Sheet1',encoding="utf-8-sig" , index = None, header=True)
#dfw['Period'] = dfw['Quarter'].apply(lambda x: r'from {0} to {1}'.format(str(x).split('/')[0],str(x).split('/')[1]))
dfwp

#dfwp = dfwp.iloc[:,['C','D','E','F','G','I','J','L','M','N','S']]
#ax1=dfwp.plot(subplots=True,sort_columns=True,figsize=(16, 32))
ax1=dfwp.plot(subplots=True, sort_columns=True, x='Quarter', y=['C','D','E','F','G','I','J','L','M','N','S'],figsize=(10, 20),rot=45,lw=2,title=['OJA by quarters NACE Level 1 Number','','','','','','','','','',''],grid=True)
fig = ax1[0].get_figure()
fig.savefig(r'{0}OJA_by_quarters_NACE_Level_1_Number.png'.format(cpath), bbox_inches = 'tight', pad_inches = 0.3)

ax2=dfwp.plot(subplots=True, x='Quarter', y=['C Change','D Change','E Change','F Change','G Change','I Change','J Change','L Change','M Change','N Change','S Change'],figsize=(10, 20),rot=45,lw=2,title=['OJA by quarters NACE Level 1 Change','','','','','','','','','',''],grid=True)
fig = ax2[0].get_figure()
fig.savefig(r'{0}OJA_by_quarters_NACE_Level_1_Change.png'.format(cpath), bbox_inches = 'tight', pad_inches = 0.3)

str.write('<h2>OJA by quarters NACE Level 1</h2>')
dfwp.to_html(buf=str, classes='table table-oja')
str.write('''
        <div>
        <img src="OJA_by_quarters_NACE_Level_1_Number.png" alt="Line chart of OJA by quarters NACE Level 1 Number" title="OJA by quarters NACE Level 1 Number"/>
        <img src="OJA_by_quarters_NACE_Level_1_Change.png" alt="Line chart of OJA by quarters NACE Level 1 Change" title="OJA by quarters NACE Level 1 Change"/>
        </div>
''')

dfw=df
dfw['Quarter'] = pd.to_datetime(dfw['data'],dayfirst='true').dt.to_period('Q')
dfw1 = dfw.groupby(['Quarter','NUTS 3'], as_index=False)['rabota'].count()
dfwp=dfw1.pivot(index='NUTS 3', columns='Quarter', values='rabota')
dfwp.rename(columns={"rabota": "Number"},inplace=True)
dfwp=dfwp.fillna(0)
dfwp = dfwp.reset_index()
dfwp=dfwp.replace([np.inf, -np.inf], np.nan)
dfwp=dfwp.fillna(0)
dfwp

str.write('<h2>OJA by quarters NUTS 3</h2>')
dfwp.to_html(buf=str, classes='table table-oja')

with open('{0}OJA_by_quarters.html'.format(cpath), 'w') as file:
    file.write(str.getvalue())
#print(html)

def send_message(message,filesToSend,emailfrom,emailto,name,dfne,server,port):
    message = r'{0}Съобщението е изпратено от Python Jupyter.'.format(message)
    msg = MIMEMultipart()
    msg["From"] = emailfrom
    msg["To"] = ",".join(emailto)
    msg["Subject"] = 'Update OJA indicators by quarters ... {0} ::: {1}'.format(name,dfne['Quarter'].iloc[-1])
    msg.preamble = 'Update OJA indicators by quarters ... {0} ::: {1}'.format(name,dfne['Quarter'].iloc[-1])
    for fileToSend in filesToSend:
        ctype, encoding = mimetypes.guess_type(fileToSend)
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        if maintype == "text":
            fp = open(path+fileToSend, encoding='utf-8')
            attachment = MIMEText(fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == "image":
            fp = open(path+fileToSend, "rb")
            attachment = MIMEImage(fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == "audio":
            fp = open(path+fileToSend, "rb")
            attachment = MIMEAudio(fp.read(), _subtype=subtype)
            fp.close()
        else:
            fp = open(path+fileToSend, "rb")
            attachment = MIMEBase(maintype, subtype)
            attachment.set_payload(fp.read())
            fp.close()
            encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
        msg.attach(attachment)
    msg.attach(MIMEText(("""\
                                <html>
                                  <head></head>
                                  <body>
                                    <p>""" + message + """</p>
                                  </body>
                                </html>
                                """).encode('utf-8'),
                             'html', _charset='utf-8'))
    server = smtplib.SMTP('mail-server-address',port)
#server.starttls()
#server.login(username,password)
    server.sendmail(emailfrom, emailto, msg.as_string())
    server.quit()

message=''
message=r'{0}{1}http://web-server-address/{2}/OJA_by_quarters.html{3}'.format(message,'Обновяване на: ',name,'<br/>')

filesToSend=[]

send_message(r'{0}'.format(message),filesToSend,emailfrom,emailto,name,dfw,server,port)