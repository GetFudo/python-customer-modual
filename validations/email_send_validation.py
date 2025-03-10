from jinja2 import Environment, FileSystemLoader
from bson.objectid import ObjectId
from search_api.settings import db, WEBSITE_URL, BASE_DIR, APP_NAME, APP_LOGO, GRPC_EMAIL_SERVICE
import os
import sys
import grpc
import datetime
from email_send_grpc import emailService_pb2, emailService_pb2_grpc
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ecomm_offers.settings")


"""
    pug file loader env
"""
env1 = Environment(loader=FileSystemLoader(
    BASE_DIR+'/email_templates/'), extensions=['pypugjs.ext.jinja.PyPugJSExtension'])


def mail_news_letter_sender(customer_email):
    template = env1.get_template("newsletter.pug")
    # ==========================send mail to clint=====================
    home_page_data = db.homepage.find_one({}, {"homepage": 1})
    if home_page_data is not None:
        if "homepage" in home_page_data:
            facebook_url = home_page_data['homepage']['facebookLink'] if "facebookLink" in home_page_data['homepage'] else ""
            twitter_url = home_page_data['homepage']['twitterLink'] if "twitterLink" in home_page_data['homepage'] else ""
            instagram_url = home_page_data['homepage']['instagramLink'] if "instagramLink" in home_page_data['homepage'] else ""
            youtube_url = home_page_data['homepage']['youtubeLink'] if "youtubeLink" in home_page_data['homepage'] else ""
        else:
            facebook_url = ""
            twitter_url = ""
            instagram_url = ""
            youtube_url = ""
    else:
        facebook_url = ""
        twitter_url = ""
        instagram_url = ""
        youtube_url = ""
    print("APP_LOGO", APP_LOGO)
    active_listing_template_html = template.render(
        company_name=APP_NAME, facebook_url=facebook_url,
        twitter_url=twitter_url, instagram_url=instagram_url, youtube_url=youtube_url,
        LOGO=APP_LOGO, website_url=WEBSITE_URL,
    )
    html_name = "newsletter.html"
    db.newsLetter.insert(
            {
                "emailAddress": customer_email,
                "timestamp": int(datetime.datetime.now().timestamp()),
                "status": 1,
                "statusMsg": "Subscribed"
            }
    )
    with open(html_name, "w") as file:
        file.write(
            active_listing_template_html)
    # ===========================send mail through sendgrid====================
    with grpc.insecure_channel(GRPC_EMAIL_SERVICE) as channel:
        stub = emailService_pb2_grpc.email_serviceStub(channel)
        subject = "You have subscribed to " + APP_NAME + "'s newsletter"
        try:
            response = stub.sendEmail(emailService_pb2.sendEmailDetails(
                emailService="SENDGRID",
                toEmail=customer_email,
                subject=subject,
                body=active_listing_template_html,
                userName="",
                trigger="News Letter SubScription",
                httpFilePath='',
                localFilePath='')
            )
            print("Greeter client received: " + response.message)
        except:
            pass
    os.remove(html_name)
    print("mail sent successfully")
    response = {
        "message": "mail sent successfully"
    }
    return response
