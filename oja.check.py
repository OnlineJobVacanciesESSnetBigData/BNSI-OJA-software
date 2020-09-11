name='oja'
fformat='.csv'
what1='jobs.bg'
what2='zaplata.bg'
url='https://www.nsi.bg'
where='\\\\path-to\\\python\\scrapy\\'
path_link=r'{0}data\\oja\\'.format(where)
where='..\\\\'
path=r'{0}data\\oja\\'.format(where)
server='mail-server-address'
port='mail-server-port'
emailfrom = "PythonJupyter@nsi.bg"
emailto = ["email1@example.com","email2@example.com","email3@example.com"]
#emailto = ["email2@example.com"]
#emailto = ["email1@example.com"]

import pandas as pd
import glob
from urllib.parse import unquote

import datetime

import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText


def load_files(d):
    message = ''
    yesterday = datetime.datetime.today()- datetime.timedelta(days = d)
    yesterday=yesterday.strftime('%Y%m%d')
#    path1 = r'{0}data\\{1}\\*_{1}_{2}*.csv'.format(where,what1,yesterday) # use your path
#    path2 = r'{0}data\\{1}\\*_{1}_{2}*.csv'.format(where,what2,yesterday) # use your path
    path1 = r'{0}data\\{1}\\*_{2}*.csv'.format(where,what1,yesterday) # use your path
    path2 = r'{0}data\\{1}\\*_{2}*.csv'.format(where,what2,yesterday) # use your path
#    all_files = glob.glob(path + "*.csv", recursive=True)
    all_files = glob.glob(path1, recursive=True)
    all_files+=glob.glob(path2, recursive=True)
    li = []
    for filename in all_files:
        print(filename)
        message = r'{0}{1}{2}{3}'.format(message,'Зареждане на файл: ',filename,'<br/>')
        df = pd.read_csv(filename,delimiter=",",usecols=['adres','zaplatamin','zapalatamax','firma','tip','ezici','zaplatavid','razglejdane','data','refnumber','valuta','grad','kategoria','nivo','tipzaetost','title','description','rabota','opisanie','obrazovanie','dyrjava','site'],encoding = "utf-8")
        df.replace(regex={r'\n': '', '\t': '', r'\s+': ' '}, inplace=True)
        li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    return frame, message

def save_files(frame,d):
    message = ''
    yesterday = datetime.datetime.today()- datetime.timedelta(days = d)
    yesterday1=yesterday.strftime('%Y%m%d')
    yesterday2=yesterday.strftime('%d.%m.%Y')
    yesterday3=yesterday.strftime('%Y-%m-%d')
    df=frame
    df.replace({'data': {' Януари ':'.01.',
    ' Февруари ':'.02.',
    ' Март ':'.03.',
    ' Април ':'.04.',
    ' Май ':'.05.',
    ' Юни ':'.06.',
    ' Юли ':'.07.',
    ' Август ':'.08.',
    ' Септември ':'.09.',
    ' Октомври ':'.10.',
    ' Ноември ':'.11.',
    ' Декември ':'.12.'
    }}, regex=True, inplace=True)
    df.replace({'data': {'\.1\.':'.01.',
    '\.2\.':'.02.',
    '\.3\.':'.03.',
    '\.4\.':'.04.',
    '\.5\.':'.05.',
    '\.6\.':'.06.',
    '\.7\.':'.07.',
    '\.8\.':'.08.',
    '\.9\.':'.09.'
    }}, regex=True, inplace=True)
    df = df.sort_values('data', ascending=True)
    try:
        dfs = pd.read_csv(r'{0}stat\\{1}_stat_files.csv'.format(path,name),delimiter=",",encoding = "utf-8")
    except:
        dfs = pd.DataFrame()
    print('Всички записи {}'.format(df.shape))
    message = r'{0}Всички записи {1}{2}'.format(message,df.shape,'<br/>')
    dfe=df.loc[~df['site'].isin(['Zaplata.bg','Jobs.bg']) | (~df['data'].str.contains('\d\d\.\d\d\.\d\d\d\d', na=False))]
    dfn=df.loc[~df['site'].isin(['Zaplata.bg','Jobs.bg']) | ~(df['data'].str.contains(yesterday2, na=False))]
    print('Записи с грешки {}'.format(dfe.shape))
    message = r'{0}Записи с грешки {1}{2}'.format(message,dfe.shape,'<br/>')
    print('Записи с грешки или за различен период {}'.format(dfn.shape))
    message = r'{0}Записи с грешки или за различен период {1}{2}'.format(message,dfn.shape,'<br/>')
    if dfe.shape[0]>0:
        dfe.to_csv(r'{0}errors//{1}_{2}_errors.csv'.format(path,name,yesterday1), index = None, header=True)
        message = r'{0}Файл с грешки: {1}errors//{2}_{3}_errors.csv{4}'.format(message,path,name,yesterday1,'<br/>')
    df=df.loc[df['site'].isin(['Zaplata.bg','Jobs.bg']) & (df['data'].str.contains(yesterday2, na=False))]
    print('Записи без грешки за {0} {1}'.format(yesterday2,df.shape))
    message = r'{0}Записи без грешки за {1} {2}{3}'.format(message,yesterday2,df.shape,'<br/>')
    df.to_csv(r'{0}{1}_{2}_noerrors.csv'.format(path,name,yesterday1), index = None, header=True)
    message = r'{0}Файл без грешки: {1}{2}_{3}_noerrors.csv{4}'.format(message,path_link,name,yesterday1,'<br/>')
    dfs = dfs.append({'data': yesterday3,
                      'all_records': frame.shape[0],
                     'records_with_errors': dfe.shape[0],
                     'records_with_errors_and_period': dfn.shape[0],
                     'good_records': df.shape[0]}, ignore_index=True)
    dfs.drop_duplicates(inplace=True)
    dfs.sort_values(by=['data'],inplace=True)
    dfs.to_csv(r'{0}stat\\{1}_stat_files.csv'.format(path,name), index = None, header=True)
    return message

def send_message(message,filesToSend,emailfrom,emailto,name,dfne,server,port):
    message = r'{0}Съобщението е изпратено от Python Jupyter.'.format(message)
    msg = MIMEMultipart()
    msg["From"] = emailfrom
    msg["To"] = ",".join(emailto)
    msg["Subject"] = 'Check scrape data ... {0} ::: {1}'.format(name,dfne['data'].iloc[0][:10])
    msg.preamble = 'Check scrape data ... {0} ::: {1}'.format(name,dfne['data'].iloc[0][:10])
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



lf=load_files(1)
sf=save_files(lf[0],1)

#for x in range(1,365):
#    lf=load_files(x)
#    sf=save_files(lf[0],x)

filesToSend=[]

send_message(r'{0}{1}'.format(lf[1],sf),filesToSend,emailfrom,emailto,name,lf[0],server,port)

