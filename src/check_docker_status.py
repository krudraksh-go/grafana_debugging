import docker
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import Counter
from datetime import datetime
from utils.DagIssueAlertUtils import push_failure_to_central_influx_mails

class DockerStatusChecker:
    def __init__(self):
        self.client = docker.from_env()

    def get_running_images_count(self):
        containers =  self.client.containers.list()
        container_image = [container.image.tags[0] if container.image.tags else container.image.id for container in containers]
        image_count = Counter(container_image)
        return image_count

    def create_email_body(self, running_images):
        html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Airflow Containers Not Running</title>
                </head>
                <body>
                    <h2>Alert: Airflow Containers Not Running</h2>
                    <p>The following Airflow containers are not running:</p>
                    <ul>
                        {"".join(f"<li>{item} : {count}</li>" for item, count in running_images.items())}
                    </ul>
                    <p>Please check the Docker status.</p>
                    <p>Thank you!</p>
                </body>
                </html>
                """
        return html

    def send_mail(self, html_body, subject, sender, receiver_list):


        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = sender
        message['To'] = ', '.join(receiver_list)
        receivers = receiver_list

        body_content = html_body
        message.attach(MIMEText(body_content, "html"))
        msg_body = message.as_string()

        server = smtplib.SMTP("email-smtp.us-east-1.amazonaws.com", 25)
        server.starttls()
        server.login('AKIA3FSTBJC6M5VPXEY3', 'BG8L0fFue6AvXFfXO4lUZMA2LoLhiq+kqyWNYmoyG+1D')
        server.sendmail(message['From'], receivers, msg_body)
        server.quit()
        print('Mail Sent!')

    def check(self):
        my_image = 'us-docker.pkg.dev/greymatter-development/apps/gm_data_flow:latest'
        running_images_count = self.get_running_images_count()

        if running_images_count[my_image]<4:
            reciever_email = ['analytics-team@greyorange.com']
            current_datetime = datetime.now()
            formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            subject = formatted_datetime + " [Alert] - Airflow Containers Not Running"
            html_body = self.create_email_body(running_images_count)
            self.send_mail(html_body, subject, 'customer_success@greyorange.com', reciever_email)
            data = {"time": formatted_datetime, "subject": subject, "html_body": "Airflow Docker containers are not running",'category':'Docker Status'}
            push_failure_to_central_influx_mails(data)


if __name__ == "__main__":
    checker = DockerStatusChecker()
    checker.check()