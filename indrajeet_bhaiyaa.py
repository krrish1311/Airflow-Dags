import smtplib
from email.message import EmailMessage

def send_mail_onfail(dag_id):
    smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_server.starttls()
    smtp_server.login('xrrishdummy@gmail.com', 'fapizjbrcbhnkhfi')

    message = EmailMessage()
    message['From'] = 'xrrishdummy@gmail.com'
    message['To'] = 'krishgoal2000@gmail.com'
    message['Subject'] = '! Dag failing ALert !!'
    message.set_content('The dag_id  has been failed')

    smtp_server.send_message(message, 'xrrishdummy@gmail.com', 'krishgoal2000@gmail.com')

    smtp_server.quit()