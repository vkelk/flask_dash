import requests
from threading import Thread
from flask import current_app, url_for, render_template
from flask_mail import Message
from itsdangerous import URLSafeTimedSerializer

from ..config import Configuration


# HELPERS
def send_mail(to_address, subject, html):
    r = requests.post("https://api.mailgun.net/v3/%s/messages" % Configuration.MAILGUN_DOMAIN,
                      auth=("api", Configuration.MAILGUN_KEY),
                      data={
                          "from": "Dashboard app <postmaster@sandboxe512714bb2924291ade762463fdedbdc.mailgun.org>",
                          "to": to_address,
                          "subject": subject,
                          "html": html
                      }
                      )
    return r


# def send_async_email(msg):
#     with app.app_context():
#         mail.send(msg)


# def send_email(subject, recipients, html_body):
#     msg = Message(subject, recipients=recipients)
#     msg.html = html_body
#     thr = Thread(target=send_async_email, args=[msg])
#     thr.start()


def send_confirmation_email(user_email):
    confirm_serializer = URLSafeTimedSerializer(Configuration.SECRET_KEY)

    confirm_url = url_for(
        'account.confirm_email',
        token=confirm_serializer.dumps(user_email, salt='email-confirmation-salt'),
        _external=True)

    html = render_template(
        'email_confirmation.html',
        confirm_url=confirm_url)

    # send_email('Confirm Your Email Address', [user_email], html)
    send_mail(user_email, 'Confirm Your Email Address', html)


def send_password_reset_email(user_email):
    password_reset_serializer = URLSafeTimedSerializer(Configuration['SECRET_KEY'])

    password_reset_url = url_for(
        'account.reset_with_token',
        token=password_reset_serializer.dumps(user_email, salt='password-reset-salt'),
        _external=True)

    html = render_template(
        'email_password_reset.html',
        password_reset_url=password_reset_url)

    # send_email('Password Reset Requested', [user_email], html)
    send_mail(user_email, 'Password Reset Requested', html)
