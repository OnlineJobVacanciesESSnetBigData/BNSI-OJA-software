name='oja'
fformat='.csv'
what=['oja']
url='https://www.nsi.bg'
where='\\\\path-to\\\python\\scrapy\\'
path=r'{0}data\\oja\\'.format(where)
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
#from urllib.parse import unquote
import numpy as np

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
    yesterday='noerrors'
    all_files=[]
    for w in what:
        path = r'{0}data\\{1}\\*_{2}*.csv'.format(where,w,yesterday) # use your path
        all_files+=glob.glob(path, recursive=True)
    li = []
    for filename in all_files:
        message = r'{0}{1}{2}{3}'.format(message,'Зареждане на файл: ',filename,'<br/>')
        df = pd.read_csv(filename,delimiter=",",usecols=['adres','zaplatamin','zapalatamax','firma','tip','ezici','zaplatavid','razglejdane','data','refnumber','valuta','grad','kategoria','nivo','tipzaetost','title','description','rabota','opisanie','obrazovanie','dyrjava','site'],encoding = "utf-8")
        df.replace(regex={r'\n': '', '\t': '', r'\s+': ' '}, inplace=True)
        li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    return frame, message


def send_message(message,filesToSend,emailfrom,emailto,name,dfne,server,port):
    message = r'{0}Съобщението е изпратено от Python Jupyter.'.format(message)
    msg = MIMEMultipart()
    msg["From"] = emailfrom
    msg["To"] = ",".join(emailto)
    msg["Subject"] = 'Encode data ... {0} ::: from {1} to {2}'.format(name,dfne['data'].iloc[0][:10],dfne['data'].iloc[-1][:10])
    msg.preamble = 'Encode data ... {0} ::: from {1} to {2}'.format(name,dfne['data'].iloc[0][:10],dfne['data'].iloc[-1][:10])
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

def encode_files(df):
    message = ''
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',df.shape,'<br/>')
    df = df.drop_duplicates(keep='first')
    message = r'{0}{1}{2}{3}'.format(message,'All records no duplicates: ',df.shape,'<br/>')
    df = df.drop_duplicates(subset={'data','firma','grad','dyrjava','rabota'},keep='first')
    message = r'{0}{1}{2}{3}'.format(message,"All records no duplicates by columns 'data','firma','grad','dyrjava','rabota': ",df.shape,'<br/>')
    
    df.replace({'obrazovanie': {r'.*Образование\s' : np.nan,
                               '\,\sКурсове.*' : np.nan,
                               r'Полувисше' : 'Висше',
                               r'ПолуВисше' : 'Висше', 
                               r'висше' : 'Висше',
                               r'Висше\sобразование' : 'Висше',
                               r'Средно\sобразование' : 'Средно',
                               r'средно' : 'Средно'}}, regex=True, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'',df.obrazovanie.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',df.obrazovanie.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',df.obrazovanie.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',df.shape,'<br/>')
    
    df['obrazovanie_en']=df['obrazovanie']
    df.replace({'obrazovanie_en': {r'Висше' : 'Higher',
                               r'Средно' : 'Secondary',
                               r'Основно' : 'Primary'}}, regex=True, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'',df.obrazovanie_en.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',df.obrazovanie_en.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',df.obrazovanie_en.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',df.shape,'<br/>')
    
    df.replace({'zaplatavid':r'бруто'}, {'zaplatavid':'Бруто'}, regex=True, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'',df.zaplatavid.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',df.zaplatavid.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',df.zaplatavid.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',df.shape,'<br/>')
    
    df.replace({'valuta':r'лв\.'}, {'valuta':'BGN'}, regex=True, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'',df.valuta.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',df.valuta.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',df.valuta.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',df.shape,'<br/>')
    
    message = r'{0}{1}{2}{3}'.format(message,'',df.site.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',df.site.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',df.site.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',df.shape,'<br/>')
    
    df['dyrjava_en']=df['dyrjava']
    message = r'{0}{1}{2}{3}'.format(message,'',df.dyrjava_en.unique().tolist(),'<br/>')
    
    df.replace({'dyrjava_en': {
'Афганистан':'Afghanistan',
'Албания':'Albania',
'Алжир':'Algeria',
'Американска Самоа':'American Samoa',
'Андора':'Andorra',
'Ангола':'Angola',
'Ангила':'Anguilla',
'Антигуа и Барбуда':'Antigua and Barbuda',
'Аржентина':'Argentina',
'Армения':'Armenia',
'Аруба':'Aruba',
'Австралия':'Australia',
'Австрия':'Austria',
'Азербайджан':'Azerbaijan',
'Бахамски острови':'Bahamas, The',
'Бахрейн':'Bahrain',
'Бангладеш':'Bangladesh',
'Барбадос':'Barbados',
'Беларус':'Belarus',
'Белгия':'Belgium',
'Белиз':'Belize',
'Бенин':'Benin',
'Бермудите':'Bermuda',
'Бутан':'Bhutan',
'Боливия':'Bolivia',
'Босна и Херцеговина':'Bosnia and Herzegovina',
'Ботсвана':'Botswana',
'Бразилия':'Brazil',
'Британски Вирджински острови':'British Virgin Islands',
'Бруней':'Brunei',
'България':'Bulgaria',
'Буркина Фасо':'Burkina Faso',
'Бирма':'Burma',
'Бурунди':'Burundi',
'Кабо Верде':'Cabo Verde',
'Камбоджа':'Cambodia',
'Камерун':'Cameroon',
'Канада':'Canada',
'Каймановите острови':'Cayman Islands',
'Централноафриканска република':'Central African Republic',
'Чад':'Chad',
'Чили':'Chile',
'Китай':'China',
'Колумбия':'Colombia',
'Коморски острови':'Comoros',
'Конго, Демократична република':'Congo, Democratic Republic of the',
'Конго, Република':'Congo, Republic of the',
'Острови Кук':'Cook Islands',
'Коста Рика':'Costa Rica',
'Кот д\'Ивоар':'Cote d\'Ivoire',
'Хърватия':'Croatia',
'Куба':'Cuba',
'Кюрасао':'Curacao',
'Кипър':'Cyprus',
'Чехия':'Czech Republic',
'Дания':'Denmark',
'Джибути':'Djibouti',
'Доминика':'Dominica',
'Доминиканска република':'Dominican Republic',
'Еквадор':'Ecuador',
'Египет':'Egypt',
'Салвадор':'El Salvador',
'Екваториална Гвинея':'Equatorial Guinea',
'Еритрея':'Eritrea',
'Естония':'Estonia',
'Етиопия':'Ethiopia',
'Фолклендските острови ( Islas Malvinas)':'Falkland Islands (Islas Malvinas)',
'Фарьорски острови':'Faroe Islands',
'Фиджи':'Fiji',
'Финландия':'Finland',
'Франция':'France',
'Френска Полинезия ':'French Polynesia',
'Габон':'Gabon',
'Гамбия, The':'Gambia, The',
'Грузия':'Georgia',
'Германия':'Germany',
'Гана':'Ghana',
'Гибралтар':'Gibraltar',
'Гърция':'Greece',
'Гренландия':'Greenland',
'Гренада':'Grenada',
'Гуам':'Guam',
'Гватемала':'Guatemala',
'Гърнси':'Guernsey',
'Гвинея-Бисау':'Guinea-Bissau',
'Гвинея':'Guinea',
'Гвиана':'Guyana',
'Хаити':'Haiti',
'Хондурас':'Honduras',
'Хонконг':'Hong Kong',
'Унгария':'Hungary',
'Исландия':'Iceland',
'Индия':'India',
'Индонезия':'Indonesia',
'Иран':'Iran',
'Ирак':'Iraq',
'Ирландия':'Ireland',
'Остров Ман':'Isle of Man',
'Израел':'Israel',
'Италия':'Italy',
'Ямайка':'Jamaica',
'Япония':'Japan',
'Джърси':'Jersey',
'Йордания':'Jordan',
'Казахстан':'Kazakhstan',
'Кения':'Kenya',
'Кирибати':'Kiribati',
'Корея, Север':'Korea, North',
'Корея, Юг':'Korea, South',
'Косово':'Kosovo',
'Кувейт':'Kuwait',
'Киргизстан':'Kyrgyzstan',
'Лаос':'Laos',
'Латвия':'Latvia',
'Ливан':'Lebanon',
'Лесото':'Lesotho',
'Либерия':'Liberia',
'Либия':'Libya',
'Лихтенщайн':'Liechtenstein',
'Литва':'Lithuania',
'Люксембург':'Luxembourg',
'Макао':'Macau',
'Македония':'Macedonia',
'Мадагаскар':'Madagascar',
'Малави':'Malawi',
'Малайзия':'Malaysia',
'Малдивите':'Maldives',
'Мали':'Mali',
'Малта':'Malta',
'Маршаловите острови':'Marshall Islands',
'Мавритания':'Mauritania',
'Мавриций':'Mauritius',
'Мексико':'Mexico',
'Микронезия, федерирани държави на':'Micronesia, Federated States of',
'Молдова':'Moldova',
'Монако':'Monaco',
'Монголия':'Mongolia',
'Черна гора':'Montenegro',
'Мароко':'Morocco',
'Мозамбик':'Mozambique',
'Намибия':'Namibia',
'Непал':'Nepal',
'Холандия':'Netherlands',
'Нова Каледония':'New Caledonia',
'Нова Зеландия':'New Zealand',
'Никарагуа':'Nicaragua',
'Нигерия':'Nigeria',
'Нигер':'Niger',
'Ниуе':'Niue',
'Северни Мариански острови':'Northern Mariana Islands',
'Норвегия':'Norway',
'Оман':'Oman',
'Пакистан':'Pakistan',
'Палау':'Palau',
'Панама':'Panama',
'Папуа Нова Гвинея':'Papua New Guinea',
'Парагвай':'Paraguay',
'Перу':'Peru',
'Филипини':'Philippines',
'Полша':'Poland',
'Португалия':'Portugal',
'Пуерто Рико':'Puerto Rico',
'Катар':'Qatar',
'Румъния':'Romania',
'Русия':'Russia',
'Руанда':'Rwanda',
'Сейнт Китс и Невис':'Saint Kitts and Nevis',
'Сейнт Лусия':'Saint Lucia',
'Сен Мартин':'Saint Martin',
'Сен Пиер и Микелон':'Saint Pierre and Miquelon',
'Сейнт Винсънт и Гренадини':'Saint Vincent and the Grenadines',
'Самоа':'Samoa',
'Сан Марино':'San Marino',
'Сао Томе и Принсипи':'Sao Tome and Principe',
'Саудитска Арабия':'Saudi Arabia',
'Сенегал':'Senegal',
'Сърбия':'Serbia',
'Сейшели':'Seychelles',
'Сиера Леоне':'Sierra Leone',
'Сингапур':'Singapore',
'Синт Маартен':'Sint Maarten',
'Словакия':'Slovakia',
'Словения':'Slovenia',
'Соломоновите острови':'Solomon Islands',
'Сомалия':'Somalia',
'Южна Африка':'South Africa',
'Южен Судан':'South Sudan',
'Испания':'Spain',
'Шри Ланка':'Sri Lanka',
'Судан':'Sudan',
'Суринам':'Suriname',
'Свазиленд':'Swaziland',
'Швеция':'Sweden',
'Швейцария':'Switzerland',
'Сирия':'Syria',
'Тайван':'Taiwan',
'Таджикистан':'Tajikistan',
'Танзания':'Tanzania',
'Тайланд':'Thailand',
'Тимор-Лесте':'Timor-Leste',
'Того':'Togo',
'Тонга':'Tonga',
'Тринидад и Тобаго':'Trinidad and Tobago',
'Тунис':'Tunisia',
'Турция':'Turkey',
'Туркменистан':'Turkmenistan',
'Тувалу':'Tuvalu',
'Уганда':'Uganda',
'Украйна':'Ukraine',
'Обединени Арабски Емирства':'United Arab Emirates',
'Дубай':'United Arab Emirates',
'Великобритания':'United Kingdom',
'United States of America':'United States',
'Съединени Американски Щати':'United States',
'Уругвай':'Uruguay',
'Узбекистан':'Uzbekistan',
'Вануату':'Vanuatu',
'Венецуела':'Venezuela',
'Виетнам':'Vietnam',
'Вирджински острови':'Virgin Islands',
'Западен бряг':'West Bank',
'Йемен':'Yemen',
'Замбия':'Zambia',
'Зимбабве':'Zimbabwe',
'Russian Federation':'Russia',
'Шипченски проход':'Bulgaria',
'Дистанционна работа':'Bulgaria',
'Кораби':'Bulgaria',
np.nan:'Bulgaria'
}}, inplace=True)

    df.replace({'dyrjava_en': {r'.*виж\sкарта.*':'Bulgaria',
                               r'.*кв\..*':'Bulgaria',
                               r'.*\/.*':'Bulgaria'}}, regex=True, inplace=True)

    df['dyrjava']=df['dyrjava_en']

    df.replace({'dyrjava': {
'Afghanistan':'Афганистан',
'Albania':'Албания',
'Algeria':'Алжир',
'American Samoa':'Американска Самоа',
'Andorra':'Андора',
'Angola':'Ангола',
'Anguilla':'Ангила',
'Antigua and Barbuda':'Антигуа и Барбуда',
'Argentina':'Аржентина',
'Armenia':'Армения',
'Aruba':'Аруба',
'Australia':'Австралия',
'Austria':'Австрия',
'Azerbaijan':'Азербайджан',
'Bahamas, The':'Бахамски острови',
'Bahrain':'Бахрейн',
'Bangladesh':'Бангладеш',
'Barbados':'Барбадос',
'Belarus':'Беларус',
'Belgium':'Белгия',
'Belize':'Белиз',
'Benin':'Бенин',
'Bermuda':'Бермудите',
'Bhutan':'Бутан',
'Bolivia':'Боливия',
'Bosnia and Herzegovina':'Босна и Херцеговина',
'Botswana':'Ботсвана',
'Brazil':'Бразилия',
'British Virgin Islands':'Британски Вирджински острови',
'Brunei':'Бруней',
'Bulgaria':'България',
'Burkina Faso':'Буркина Фасо',
'Burma':'Бирма',
'Burundi':'Бурунди',
'Cabo Verde':'Кабо Верде',
'Cambodia':'Камбоджа',
'Cameroon':'Камерун',
'Canada':'Канада',
'Cayman Islands':'Каймановите острови',
'Central African Republic':'Централноафриканска република',
'Chad':'Чад',
'Chile':'Чили',
'China':'Китай',
'Colombia':'Колумбия',
'Comoros':'Коморски острови',
'Congo, Democratic Republic of the':'Конго, Демократична република',
'Congo, Republic of the':'Конго, Република',
'Cook Islands':'Острови Кук',
'Costa Rica':'Коста Рика',
'Cote d\'Ivoire':'Кот д\'Ивоар',
'Croatia':'Хърватия',
'Cuba':'Куба',
'Curacao':'Кюрасао',
'Cyprus':'Кипър',
'Czech Republic':'Чехия',
'Denmark':'Дания',
'Djibouti':'Джибути',
'Dominica':'Доминика',
'Dominican Republic':'Доминиканска република',
'Ecuador':'Еквадор',
'Egypt':'Египет',
'El Salvador':'Салвадор',
'Equatorial Guinea':'Екваториална Гвинея',
'Eritrea':'Еритрея',
'Estonia':'Естония',
'Ethiopia':'Етиопия',
'Falkland Islands (Islas Malvinas)':'Фолклендските острови ( Islas Malvinas)',
'Faroe Islands':'Фарьорски острови',
'Fiji':'Фиджи',
'Finland':'Финландия',
'France':'Франция',
'French Polynesia':'Френска Полинезия ',
'Gabon':'Габон',
'Gambia, The':'Гамбия, The',
'Georgia':'Грузия',
'Germany':'Германия',
'Ghana':'Гана',
'Gibraltar':'Гибралтар',
'Greece':'Гърция',
'Greenland':'Гренландия',
'Grenada':'Гренада',
'Guam':'Гуам',
'Guatemala':'Гватемала',
'Guernsey':'Гърнси',
'Guinea-Bissau':'Гвинея-Бисау',
'Guinea':'Гвинея',
'Guyana':'Гвиана',
'Haiti':'Хаити',
'Honduras':'Хондурас',
'Hong Kong':'Хонконг',
'Hungary':'Унгария',
'Iceland':'Исландия',
'India':'Индия',
'Indonesia':'Индонезия',
'Iran':'Иран',
'Iraq':'Ирак',
'Ireland':'Ирландия',
'Isle of Man':'Остров Ман',
'Israel':'Израел',
'Italy':'Италия',
'Jamaica':'Ямайка',
'Japan':'Япония',
'Jersey':'Джърси',
'Jordan':'Йордания',
'Kazakhstan':'Казахстан',
'Kenya':'Кения',
'Kiribati':'Кирибати',
'Korea, North':'Корея, Север',
'Korea, South':'Корея, Юг',
'Kosovo':'Косово',
'Kuwait':'Кувейт',
'Kyrgyzstan':'Киргизстан',
'Laos':'Лаос',
'Latvia':'Латвия',
'Lebanon':'Ливан',
'Lesotho':'Лесото',
'Liberia':'Либерия',
'Libya':'Либия',
'Liechtenstein':'Лихтенщайн',
'Lithuania':'Литва',
'Luxembourg':'Люксембург',
'Macau':'Макао',
'Macedonia':'Македония',
'Madagascar':'Мадагаскар',
'Malawi':'Малави',
'Malaysia':'Малайзия',
'Maldives':'Малдивите',
'Mali':'Мали',
'Malta':'Малта',
'Marshall Islands':'Маршаловите острови',
'Mauritania':'Мавритания',
'Mauritius':'Мавриций',
'Mexico':'Мексико',
'Micronesia, Federated States of':'Микронезия, федерирани държави на',
'Moldova':'Молдова',
'Monaco':'Монако',
'Mongolia':'Монголия',
'Montenegro':'Черна гора',
'Morocco':'Мароко',
'Mozambique':'Мозамбик',
'Namibia':'Намибия',
'Nepal':'Непал',
'Netherlands':'Холандия',
'New Caledonia':'Нова Каледония',
'New Zealand':'Нова Зеландия',
'Nicaragua':'Никарагуа',
'Nigeria':'Нигерия',
'Niger':'Нигер',
'Niue':'Ниуе',
'Northern Mariana Islands':'Северни Мариански острови',
'Norway':'Норвегия',
'Oman':'Оман',
'Pakistan':'Пакистан',
'Palau':'Палау',
'Panama':'Панама',
'Papua New Guinea':'Папуа Нова Гвинея',
'Paraguay':'Парагвай',
'Peru':'Перу',
'Philippines':'Филипини',
'Poland':'Полша',
'Portugal':'Португалия',
'Puerto Rico':'Пуерто Рико',
'Qatar':'Катар',
'Romania':'Румъния',
'Russia':'Русия',
'Rwanda':'Руанда',
'Saint Kitts and Nevis':'Сейнт Китс и Невис',
'Saint Lucia':'Сейнт Лусия',
'Saint Martin':'Сен Мартин',
'Saint Pierre and Miquelon':'Сен Пиер и Микелон',
'Saint Vincent and the Grenadines':'Сейнт Винсънт и Гренадини',
'Samoa':'Самоа',
'San Marino':'Сан Марино',
'Sao Tome and Principe':'Сао Томе и Принсипи',
'Saudi Arabia':'Саудитска Арабия',
'Senegal':'Сенегал',
'Serbia':'Сърбия',
'Seychelles':'Сейшели',
'Sierra Leone':'Сиера Леоне',
'Singapore':'Сингапур',
'Sint Maarten':'Синт Маартен',
'Slovakia':'Словакия',
'Slovenia':'Словения',
'Solomon Islands':'Соломоновите острови',
'Somalia':'Сомалия',
'South Africa':'Южна Африка',
'South Sudan':'Южен Судан',
'Spain':'Испания',
'Sri Lanka':'Шри Ланка',
'Sudan':'Судан',
'Suriname':'Суринам',
'Swaziland':'Свазиленд',
'Sweden':'Швеция',
'Switzerland':'Швейцария',
'Syria':'Сирия',
'Taiwan':'Тайван',
'Tajikistan':'Таджикистан',
'Tanzania':'Танзания',
'Thailand':'Тайланд',
'Timor-Leste':'Тимор-Лесте',
'Togo':'Того',
'Tonga':'Тонга',
'Trinidad and Tobago':'Тринидад и Тобаго',
'Tunisia':'Тунис',
'Turkey':'Турция',
'Turkmenistan':'Туркменистан',
'Tuvalu':'Тувалу',
'Uganda':'Уганда',
'Ukraine':'Украйна',
'United Arab Emirates':'Обединени Арабски Емирства',
'United Kingdom':'Великобритания',
'United States of America':'Съединени Американски Щати',
'United States':'Съединени Американски Щати',
'Uruguay':'Уругвай',
'Uzbekistan':'Узбекистан',
'Vanuatu':'Вануату',
'Venezuela':'Венецуела',
'Vietnam':'Виетнам',
'Virgin Islands':'Вирджински острови',
'West Bank':'Западен бряг',
'Yemen':'Йемен',
'Zambia':'Замбия',
'Zimbabwe':'Зимбабве',
'Russian Federation':'Русия',
np.nan:'България'
}}, inplace=True)



    message = r'{0}{1}{2}{3}'.format(message,'',df.dyrjava_en.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',df.dyrjava_en.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',df.dyrjava_en.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',df.shape,'<br/>')
    
    cc = pd.read_csv(r"{0}..\\classifications\\country_codes_bg.csv".format(where),delimiter=",",usecols=['COUNTRY','CODE'],encoding = "utf-8")
    cc.rename(columns={"COUNTRY": "dyrjava_en", "CODE": "dyrjava_id"}, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'',cc.dyrjava_en.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',cc.shape,'<br/>')
    
    dfc = pd.merge(df, cc, on=['dyrjava_en'], how='left')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfc.shape,'<br/>')
    
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.dyrjava_id.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.dyrjava_en.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',dfc.dyrjava.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',df.dyrjava.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfc.shape,'<br/>')
    
    dfc['tip_rabota_bg']=df['tipzaetost']
    dfc.tip_rabota_bg.replace(".*Постоянна.*", "Постоянна работа", regex=True, inplace=True)
    dfc.tip_rabota_bg.replace(".*Временна.*", "Временна работа", regex=True, inplace=True)
    dfc.tip_rabota_bg.replace(".*Стаж.*", "Временна работа", regex=True, inplace=True)
    dfc.tip_rabota_bg.replace(".*работно.*", np.nan, regex=True, inplace=True)
    dfc.tip_rabota_bg.replace(".*Образование Средно.*", "Постоянна работа", regex=True, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.tip_rabota_bg.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.tip_rabota_bg.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',dfc.tip_rabota_bg.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfc.shape,'<br/>')
    
    dfc['tip_rabota_en']=dfc['tip_rabota_bg']
    dfc.tip_rabota_en.replace(".*Постоянна.*", "Permanent job", regex=True, inplace=True)
    dfc.tip_rabota_en.replace(".*Временна.*", "Temporary work", regex=True, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.tip_rabota_en.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.tip_rabota_en.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',dfc.tip_rabota_en.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfc.shape,'<br/>')
    
    dfc['rabotno_vreme_bg']=df['tipzaetost']
    dfc.rabotno_vreme_bg.replace(".*Пълно работно време.*", "Пълно работно време", regex=True, inplace=True)
    dfc.rabotno_vreme_bg.replace(".*Пълен Работен Ден.*", "Пълно работно време", regex=True, inplace=True)
    dfc.rabotno_vreme_bg.replace(".*Непълно работно време.*", "Непълно работно време", regex=True, inplace=True)
    dfc.rabotno_vreme_bg.replace(".*Непълен Работен Ден\/Почасова.*", "Непълно работно време", regex=True, inplace=True)
    dfc.rabotno_vreme_bg.replace(".*работа.*", np.nan, regex=True, inplace=True)
    dfc.rabotno_vreme_bg.replace(".*пълно или непълно.*", "Пълно работно време", regex=True, inplace=True)
    dfc.rabotno_vreme_bg.replace(".*Образование Средно.*", "Пълно работно време", regex=True, inplace=True)
    dfc.rabotno_vreme_bg.replace(".*непълно работно време.*", "Непълно работно време", regex=True, inplace=True)
    dfc.rabotno_vreme_bg.replace(".*без опит.*", "Пълно работно време", regex=True, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.rabotno_vreme_bg.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.rabotno_vreme_bg.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',dfc.rabotno_vreme_bg.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfc.shape,'<br/>')
    
    dfc['rabotno_vreme_en']=dfc['rabotno_vreme_bg']
    dfc.rabotno_vreme_en.replace(".*Пълно работно време.*", "Full time work", regex=True, inplace=True)
    dfc.rabotno_vreme_en.replace(".*Непълно работно време.*", "Part-time work", regex=True, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.rabotno_vreme_en.unique().tolist(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'',dfc.rabotno_vreme_en.value_counts(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',dfc.rabotno_vreme_en.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfc.shape,'<br/>')
    
    dfe = pd.read_csv(r"{0}..\\classifications\\ek_atte.csv".format(where),
                  delimiter=";",encoding = "utf-8", converters={'ekatte': lambda x: str(x),
                                                                'kind': lambda x: str(x),
                                                                'category': lambda x: str(x),
                                                                'altitude': lambda x: str(x),
                                                                'document': lambda x: str(x),
                                                                'abc': lambda x: str(x)})
    dfe.rename(columns={"name": "Location"}, inplace=True)
    dfe.sort_values(by=['t_v_m'], inplace=True)
# Махане на дублирани имена на населени места и оставяне само на първото
    dfe = dfe.drop_duplicates(subset={'Location'},keep='first')
#########################################################################
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfe.shape,'<br/>')
    
    dfc.rename(columns={"grad": "Location"}, inplace=True)
#cols = ['Location']
#dfd = dfc.join(dfe.set_index(cols), on=cols)
    dfd = pd.merge(dfc, dfe, on=['Location'], how='left')
    dfd.drop(columns=['abc'], inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',dfd.ekatte.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfd.shape,'<br/>')
    
    dfn = pd.read_csv(r"{0}..\\classifications\\NUTS3_EKATTE.csv".format(where),
                  delimiter=";",encoding = "utf-8", converters={'abc': lambda x: str(x)})
    dfn.rename(columns={"EKATTE_OBL": "oblast"}, inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfn.shape,'<br/>')
    
    cols = ['oblast']
    dfn = dfd.join(dfn.set_index(cols), on=cols)
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',dfn.oblast.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfn.shape,'<br/>')
    
    li1 = []
    filename1=r'{0}..\\classifications\\ikt2018big.csv'.format(where)
    filename2=r'{0}..\\classifications\\ikt2018small.csv'.format(where)
    df1 = pd.read_csv(filename1,delimiter=",",encoding = "utf-8",
                  usecols=['EIK','NAME','KID1_08','KID2_08','KID3_08','KID4_08','V16110_ZL'],
                  converters={'EIK': lambda x: str(x),
                                                                'KID1_08': lambda x: str(x),
                                                                'KID2_08': lambda x: str(x),
                                                                'KID3_08': lambda x: str(x),
                                                                'KID4_08': lambda x: str(x),
                                                                'V16110_ZL': lambda x: str(x)})
    li1.append(df1)
    df2 = pd.read_csv(filename2,delimiter=",",encoding = "utf-8",
                  usecols=['EIK','NAME','KID1_08','KID2_08','KID3_08','KID4_08','V16110_ZL'],
                  converters={'EIK': lambda x: str(x),
                                                                'KID1_08': lambda x: str(x),
                                                                'KID2_08': lambda x: str(x),
                                                                'KID3_08': lambda x: str(x),
                                                                'KID4_08': lambda x: str(x),
                                                                'V16110_ZL': lambda x: str(x)})
    li1.append(df2)

    frame1 = pd.concat(li1, axis=0, ignore_index=True)
    frame1.rename(columns={"NAME": "firma1"}, inplace=True)
    frame1 = frame1.drop_duplicates(subset={'EIK'},keep='first')
    frame1.firma1.str.strip()
    frame1.shape
    
    frame2 = frame1.drop_duplicates(subset={'firma1'},keep='first')
    frame2.shape
    
    dfn.firma.str.strip()
    dfn['firma1']=dfn['firma']
    dfn.firma1.str.strip()
    cols = ['firma1']
    dfe = dfn.join(frame2.set_index(cols), on=cols)
    dfe.drop(columns=['firma1'], inplace=True)
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',dfe.EIK.count(),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfe.shape,'<br/>')
    
    dfe.to_csv(r'{0}stat\\oja.encode.files.all.csv'.format(path), columns=['rabota','obrazovanie','obrazovanie_en','dyrjava','dyrjava_en','dyrjava_id','zaplatamin','zapalatamax','firma','ezici','zaplatavid','data','valuta','ekatte','t_v_m','Location','oblast','obstina','kmetstvo','kind','category','altitude','document','tsb','abc','NUTS3_CODE','NUTS3_NAME_BG','NUTS3_NAME_LAT','tip_rabota_bg','tip_rabota_en','rabotno_vreme_bg','rabotno_vreme_en','EIK','KID1_08','KID2_08','KID3_08','KID4_08','V16110_ZL','site'], index = None, header=True)
    message = r'{0}{1}{2}{3}'.format(message,'Записване на всички във файл: ',r'{0}stat\\oja.encode.files.all.csv'.format(path),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',dfe.shape,'<br/>')
    bgr=dfe[dfe.dyrjava_id == 'BGR']
    bgr.to_csv(r'{0}stat\\oja.encode.files.bg.csv'.format(path), columns=['rabota','obrazovanie','obrazovanie_en','dyrjava','dyrjava_en','dyrjava_id','zaplatamin','zapalatamax','firma','ezici','zaplatavid','data','valuta','ekatte','t_v_m','Location','oblast','obstina','kmetstvo','kind','category','altitude','document','tsb','abc','NUTS3_CODE','NUTS3_NAME_BG','NUTS3_NAME_LAT','tip_rabota_bg','tip_rabota_en','rabotno_vreme_bg','rabotno_vreme_en','EIK','KID1_08','KID2_08','KID3_08','KID4_08','V16110_ZL','site'], index = None, header=True)
    message = r'{0}{1}{2}{3}'.format(message,'Записване на България във файл: ',r'{0}stat\\oja.encode.files.bg.csv'.format(path),'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'All records: ',bgr.shape,'<br/>')
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',bgr.count(),'<br/>')
    
    message = r'{0}{1}{2}{3}'.format(message,'Count: ',frame1.firma1.value_counts(),'<br/>')
    
    bgrn=bgr.dropna(subset=['EIK'])
    bgrn.shape
    
    return message

lf=load_files(1)

ef=encode_files(lf[0])
print(ef.replace('<br/>','\n'))

filesToSend=[]

send_message(r'{0}{1}'.format(lf[1],ef),filesToSend,emailfrom,emailto,name,lf[0],server,port)
