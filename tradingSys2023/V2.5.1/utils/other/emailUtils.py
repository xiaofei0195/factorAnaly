# -*- coding:utf-8 -*-
import sys
import os
# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

fromAddr ='xiaofei0195@qq.com'
password = 'yfpyhcsmisoubjei'
toAddr ='xiaofei0195@qq.com'
# ,'277049080@qq.com','fanming@jjcz14.wecom.work'
toAddrList =['xiaofei0195@qq.com']

def makeMail(subject,content):
    mail = MIMEText(content, 'plain', 'utf-8')
    mail['From'] = fromAddr
    mail['To'] = ','.join(toAddrList)
    # mail['To'] = Header(toAddr, 'utf-8')
    mail['Subject'] = Header(subject, 'utf-8')
    return mail

def makeAttrMail(subject,content):
    mail = MIMEMultipart()
    mail['From'] = fromAddr
    mail['To'] = ','.join(toAddrList)
    # mail['To'] = Header(toAddr, 'utf-8')
    mail['Subject'] = Header(subject, 'utf-8')
    mail.attach(MIMEText(content, 'plain', 'utf-8'))
    return mail

def sendMail(mail):
    server = smtplib.SMTP_SSL("smtp.qq.com")
    server.login(fromAddr, password)
    server.sendmail(fromAddr,  mail['To'].split(','), mail.as_string())
    server.quit()

def makeAttachment(filename):
    file = open(filename, 'rb')
    blob = file.read()
    attachment = MIMEText(blob, 'base64', 'utf-8')
    attachment["Content-Type"] = 'application/octet-stream'
    attachment["Content-Disposition"] = 'attachment; filename="%s"' % filename
    return attachment

def makeAttMail(subject,content):
    mail = MIMEMultipart()
    mail['From'] = Header(fromAddr, 'utf-8')
    mail['To'] = Header(toAddr, 'utf-8')
    mail['Subject'] = Header(subject, 'utf-8')
    mail.attach(MIMEText(content, 'plain', 'utf-8'))
    return mail
