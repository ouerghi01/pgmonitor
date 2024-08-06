import psycopg2
import select
from threading import Thread
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import multiprocessing
import csv

class EmailSender:
    def __init__(self):
        self.dbname = "bench"
        self.user = "aziz"
        self.password = "123"
        self.email="mohamedaziz.ouerghi@etudiant-enit.utm.tn"
        self.password_email="14656747"
        self.to_email=self.email
        self.conn = psycopg2.connect(f"dbname={self.dbname} user={self.user} password={self.password}")
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cur = self.conn.cursor()
        self.cur.execute("LISTEN query_changes;")
    def execute_trigger(self):
        schema = "public"
        self.cur.execute(f"""
              SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = '{schema}'
              AND table_type = 'BASE TABLE';
              """)
        tables = self.cur.fetchall()
        for table in tables:
            table_name = table[0]
            trigger_name = f"{table_name}_query_changes_trigger"
            self.cur.execute(f"""
        CREATE TRIGGER {trigger_name}
        AFTER INSERT OR UPDATE OR DELETE ON {schema}.{table_name}
        FOR EACH ROW EXECUTE FUNCTION notify_trigger();
         """)
        print(f"Trigger created for table: {table_name}")
    def collect_pg_stat_statements(self):
        self.cur.execute("SELECT * FROM pg_stat_statements;")
        new_data = self.cur.fetchall()
        col_names = [desc[0] for desc in self.cur.description]
        file_exists = os.path.isfile("pg_stat_statements.csv")
        with open("pg_stat_statements.csv", "a", newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(col_names) 
            writer.writerows(new_data)
    def sendEmail(self, body):
        try:
          payload = json.loads(body)
          subject = f"Changes in {payload['table']} table"
          text = f"Operation: {payload['operation']}\nOld Data: {json.dumps(payload['old_data'], indent=2)}\nNew Data: {json.dumps(payload['new_data'], indent=2)}"
          msg = MIMEMultipart()
          msg['From'] = self.email
          msg['To'] = self.to_email
          msg['Subject'] = subject
          msg.attach(MIMEText(text, 'plain'))

          server = smtplib.SMTP('smtp.gmail.com', 587)
          server.starttls()
         
          server.login(self.email, self.password_email)
          server.sendmail(self.email, self.to_email, msg.as_string())
          server.quit()
        except Exception as e:
          print(f"Failed to send email: {e}")
    def sendAnyData(self, body):
       try:
          payload = json.loads(body)
          subject = 'Notification'
          text = f"Received Data:\n\n{json.dumps(payload, indent=2)}"
          msg = MIMEMultipart()
          msg['From'] = self.email
          msg['To'] = self.to_email
          msg['Subject'] = subject
          msg.attach(MIMEText(text, 'plain'))

          server = smtplib.SMTP('smtp.gmail.com', 587)
          server.starttls()
         
          server.login(self.email, self.password_email)
          server.sendmail(self.email, self.to_email, msg.as_string())
          server.quit()
       except Exception as e:
           print(f"Failed to send email: {e}")

       
    def listen_notifications(self):
        self.conn.poll()
        while self.conn.notifies:
            notify = self.conn.notifies.pop(0)
            t = Thread(target=self.sendEmail, args=(notify.payload,))
            t.start()
    def run(self):
        while True:
         if select.select([self.conn], [], [], 5) == ([], [], []):
            pass
         else:
            self.listen_notifications()

def NotificationOn():
   print("notify started")
   emailSender=EmailSender()
   emailSender.run()
def CollectPgStatStatements():
    emailSender=EmailSender()
    emailSender.collect_pg_stat_statements()