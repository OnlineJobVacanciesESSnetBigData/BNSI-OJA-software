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

df = pd.read_csv(r'{0}stat\\{1}_stat_files.csv'.format(path,name),delimiter=",",encoding = "utf-8")
df.shape

dfw=df
dfw['Month'] = pd.to_datetime(dfw['data'],dayfirst='true').dt.to_period('M')
dfw = dfw.groupby(['Month'], as_index=False)['good_records'].sum()
dfw.rename(columns={"good_records": "Number"},inplace=True)
dfw['Change'] = dfw['Number'].pct_change()
dfw.to_csv(r'{0}OJA_data_by_months.csv'.format(cpath), index = None, header=True)
dfw.to_excel(r'{0}OJA_data_by_months.xlsx'.format(cpath), sheet_name='Sheet1',encoding="utf-8-sig" , index = None, header=True)
#dfw['Period'] = dfw['Month'].apply(lambda x: r'from {0} to {1}'.format(str(x).split('/')[0],str(x).split('/')[1]))
#dfh=dfw.style.set_caption('OJA data by months')
#dfh

ax1=dfw.plot(x='Month', y='Number',figsize=(16, 8),rot=45,lw=5,title='Number of OJA by months',grid=True)
#ax.set(xlabel="x label", ylabel="y label")
fig = ax1.get_figure()
fig.savefig(r'{0}Number_of_OJA_by_months.png'.format(cpath))

ax2=dfw.plot(x='Month', y='Change',figsize=(16, 8),rot=45,lw=5,title='Chage of OJA by months',grid=True)
fig = ax2.get_figure()
fig.savefig(r'{0}Chage_of_OJA_by_months.png'.format(cpath))

str = io.StringIO()
dfw.to_html(buf=str, classes='table table-oja')
html = str.getvalue()
html = '''<html>
    <head>
        <title>OJA data by months</title>
        <link type="text/css" rel="stylesheet" href="oja.css" media="all" />
    </head>
    <body>
        <h1>OJA data by months</h1>
        <div>
        <ul>Download data:
        <li>
        <a href="OJA_data_by_months.csv" title="Download OJA data by months in CSV">OJA_data_by_months.csv</a>
        </li>
        <li>
        <a href="OJA_data_by_months.xlsx" title="Download OJA data by months in XLSX">OJA_data_by_months.xlsx</a>
        </li>
        </ul>
        </div>
        <div>
        <img src="Number_of_OJA_by_months.png" alt="Line chart of Number of OJA by months" title="Number of OJA by months"/>
        <img src="Chage_of_OJA_by_months.png" alt="Line chart of Chage of OJA by months" title="Chage of OJA by months"/>
        </div>
        {0}
    </body>
</html>'''.format(html)
with open('{0}OJA_data_by_months.html'.format(cpath), 'w') as file:
    file.write(html)
print(html)

def send_message(message,filesToSend,emailfrom,emailto,name,dfne,server,port):
    message = r'{0}Съобщението е изпратено от Python Jupyter.'.format(message)
    msg = MIMEMultipart()
    msg["From"] = emailfrom
    msg["To"] = ",".join(emailto)
    msg["Subject"] = 'Update OJA data by months ... {0} ::: {1}'.format(name,dfne['Month'].iloc[-1])
    msg.preamble = 'Update OJA data by months ... {0} ::: {1}'.format(name,dfne['Month'].iloc[-1])
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
message=r'{0}{1}http://web-server-address/{2}/OJA_data_by_months.html{3}'.format(message,'Обновяване на: ',name,'<br/>')

filesToSend=[]

send_message(r'{0}'.format(message),filesToSend,emailfrom,emailto,name,dfw,server,port)