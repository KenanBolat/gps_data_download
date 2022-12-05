import smtplib
import ssl
from dotenv import load_dotenv

load_dotenv()

EMAIL_USERNAME = os.getenv('EMAIL_USERNAME')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')


class SendEmail(object):
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"
        self.port = 587  # For starttls
        self.sender_email = EMAIL_USERNAME
        self.receiver_email = ["gktnotification@gmail.com", "kenan23@gmail.com"]
        self.__password = EMAIL_PASSWORD
        self.__context = None

    @property
    def context(self):
        return self.__context

    @context.setter
    def context(self, value):
        self.__context = value

    def send_email(self, context):

        # Create a secure SSL context
        context = ssl.create_default_context()
        message = """\  
               Subject: Hi there  This message is sent from Python."""

        # Try to log in to server and send email
        try:
            server = smtplib.SMTP(smtp_server, port)
            server.ehlo()  # Can be omitted
            server.starttls(context=context)  # Secure the connection
            server.ehlo()  # Can be omitted
            server.login(self.sender_email, self.password)
            server.sendmail(self.sender_email, self.receiver_email, message)
            # TODO: Send email here
        except Exception as e:
            # Print any error messages to stdout
            print(e)
        finally:
            server.quit()
