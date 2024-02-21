#!/usr/bin/env python3
import json, requests, argparse, sys, slack
from collections import namedtuple
import config
from time import time, sleep
from datetime import datetime, timedelta
from dateutil import parser
from jinja2 import Template
from models import Base, History, User, Notification, Message, HistoryNotification
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Session = None
session = requests.Session()
GALAXY_BASEURL: str
GALAXY_API_KEY: str
GALAXY_HIST_VIEW_BASE: str
NULL_USER_DETAILS = {"Status":"Not Available"}
SLACK_CLIENT = slack.WebClient(token=config.SLACK_TOKEN)

argparser = argparse.ArgumentParser(description='Manage user histories in Galaxy')
argparser.add_argument('-d', '--dryrun', action='store_const',const=True, default=False, help="Do a dry run. List affected users, but do not send emails or delete histories")
argparser.add_argument('-w', '--warn', action='store_const',const=True, default=False, help="Do a history scan and send warning emails to affected user")
argparser.add_argument('--delete', action='store_const',const=True, default=False, help="Do a history scan, send emails and delete eligible histories.")
argparser.add_argument('--force', action='store_const',const=True, default=False, help="Force a run even if last run was less than configured threshold.")
argparser.add_argument('--production', action='store_const',const=True, default=False, help="Act on the production server instead of the staging server by default")
argparser.add_argument('--notify', action='store_const',const=True, default=False, help="Post results to Slack")
argparser.add_argument('--drop_db', action='store_const',const=True, default=False, help="Drop associated database. Does not do processing.")
argparser.add_argument('--purge', action='store_const',const=True, default=False, help="Purges previously deleted histories.")


def notify_slack(title, msg, colour):
  global SLACK_CLIENT

  data = {}
  data['title']=" ".join([title, config.SLACK_LOG_MENTIONS])
  data['color']=colour
  data['text']=msg

  SLACK_CLIENT.chat_postMessage(
    channel=config.SLACK_LOG_CHANNEL,
    attachments=json.dumps([data])
  )
  return

def get_all_histories(warn_days, published="False",limit=100,keys=config.GALAXY_DEFAULT_KEYS):
  global GALAXY_BASEURL
  global GALAXY_API_KEY
  print("Querying histories...")
  start=time()
  wt = datetime.now() - timedelta(days=warn_days)
  apiURL = GALAXY_BASEURL + config.GALAXY_HISTORIES_EP
  queryURL = apiURL+'?all=true&key='+ GALAXY_API_KEY + '&q=purged&qv=False&q=published&qv=' + published + \
    '&q=update_time-le&qv=' + str(wt.isoformat()) + '&keys=' + keys + '&limit=' + str(limit)
  ret = []
  queries_left=True
  offset=0

  while queries_left:
    res=session.get(queryURL+ '&offset=' + str(offset))

    if res.status_code != 200:
      print("ERROR: Request did not return ok: " + res.reason + ': ' + res.text)
      return False

    for response in res.json():
      response['update_time'] = parser.parse(response['update_time'])
      ret.append(response)

    offset += limit
    queries_left = len(res.json()) > 0
    sys.stdout.write("Received histories: " + str(len(ret)) + "   \r")
    sys.stdout.flush()

  print(str(len(ret)) + " histories returned. Query took: " + str(timedelta(seconds=time()-start)))
  return ret

def filter_histories_update_time(histories, warn_days, delete_days):
  wt = datetime.now() - timedelta(days=warn_days)
  dt = datetime.now() - timedelta(days=delete_days)
  warn_ret = []
  delete_ret = []
  for history in histories:
    ut = history['update_time']
    if ut < wt:
      if ut < dt:
        delete_ret.append(history)
      else:
        warn_ret.append(history)

  return warn_ret, delete_ret

def culm_days(days):
  culm_size = 0.0
  ret = {}
  for day in days.keys():
    culm_size += days[day]
    ret[day] = culm_size

  return ret

def sizeof_fmt(num, suffix='B'):
  for unit in ['','K','M','G','T','P']:
    if abs(num) < 1024.0:
      return "%3.1f%s%s" % (num, unit, suffix)
    num /= 1024.0
  return "%.1f%s%s" % (num, 'Yi', suffix)

def culminate_histories_size(histories):
  history_bytes = 0.0
  for history in histories:
    history_bytes += history['size']
  return history_bytes

def process_size(histories, label="delete eligible"):
  ret = "Total space used by " + label + " histories: " + sizeof_fmt(culminate_histories_size(histories))
  print(ret)
  return ret

def get_user_details(user_id):
  global GALAXY_BASEURL
  global GALAXY_API_KEY
  queryURL = GALAXY_BASEURL + config.GALAXY_USER_EP + '/' + user_id

  res=session.get(queryURL+'?key='+ GALAXY_API_KEY)

  if res.status_code != 200:
    print("ERROR: Request did not return ok: " + res.reason + ': ' + res.text)
    return False

  ret = res.json()
  ret.pop('tags_used', None)
  ret.pop('preferences', None)
  return ret

def add_user_groups(users):
  global GALAXY_BASEURL
  global GALAXY_API_KEY

  queryURL = GALAXY_BASEURL + config.GALAXY_GROUP_EP
  res=session.get(queryURL+'?key='+ GALAXY_API_KEY)

  if res.status_code != 200:
    print("ERROR: Request did not return ok: " + res.reason + ': ' + res.text)
    return False

  groups = res.json()
  group_i = 0
  start=time()
  for group in groups:
    group_i += 1
    sys.stdout.write("Populating group: " + group['name'] + " (" + str(group_i) + "/" + str(len(groups)) + ")   \r")
    sys.stdout.flush()
    queryURL = GALAXY_BASEURL + config.GALAXY_GROUP_EP + group['id'] + config.GALAXY_GROUP_USER_EP
    res=session.get(queryURL+'?key='+ GALAXY_API_KEY)

    if res.status_code != 200:
      print("ERROR: Request did not return ok: " + res.reason + ': ' + res.text)
      return False

    group_users = res.json()

    for user in group_users:
      if user['id'] in users.keys():
        if 'groups' in users[user['id']]['details'].keys():
          users[user['id']]['details']['groups'].append(group)
        else:
          users[user['id']]['details']['groups'] = [group]

  print(str(len(groups)) + " groups queried. Total query time: " + str(timedelta(seconds=time()-start)))
  return

def send_email(to=[], html="", subject=config.MAIL_SUBJECT_WARNING, from_address=config.MAIL_FROM, replyto=config.MAIL_REPLYTO, production=False):
  if len(to) == 0:
    print("ERROR: No to address specified; aborting email send")
    return False

  if html == "":
    print("ERROR: No html body specified; aborting email send")
    return False

  headers = {}
  headers['X-Server-API-Key'] = config.MAIL_API
  headers['Content-type'] = 'application/json'

  payload = {}
  if production:
    payload['to'] = to
  else:
    payload['to'] = ['ga_au_mailer_dev@maildrop.cc']
  payload['html_body'] = html
  payload['from'] = from_address
  payload['subject'] = subject
  payload['reply_to'] = replyto

  postURL = config.MAIL_BASEURL + config.MAIL_SENDMESSAGE
  res = session.post(postURL, headers=headers, data=json.dumps(payload))

  if res.status_code != 200:
    ret = {}
    ret['status'] = ','.join([str(res.status_code), res.reason, res.text])
    return ret

  return res.json()

def remove_history(history, purge=False):
  global GALAXY_BASEURL; global GALAXY_API_KEY

  apiURL = GALAXY_BASEURL + config.GALAXY_HISTORIES_EP +  "/" + history
  queryURL = apiURL+'?key='+ GALAXY_API_KEY + '&purge=' + str(purge)
  res=session.delete(queryURL)
  return res.status_code == 200

def get_users_details(user_ids, histories):
    #Given a set of user ids, return a dictionary of user details for each with their associated histories
    global Session
    global NULL_USER_DETAILS

    print("Building user information")
    users = {}
    bad_users = {}
    user_count = 0
    start=time()
    db_session = Session()
    for uid in user_ids:
      user = {}
      user['histories'] = []
      if uid is None:
        details = None
      else:
        details = get_user_details(uid)

      if details:
        u_model = db_session.query(User).filter_by(id=details['id']).first()
        if u_model is None:
          db_session.add(User(details))
          db_session.commit()
        else:
          u_model.update(details)
          db_session.add(u_model)
          db_session.commit()
        user['details'] = details
        users[uid] = user
      else:
        user['details'] = NULL_USER_DETAILS
        bad_users[uid] = user

      if user_count % 100 == 0:
        sys.stdout.write("Users queried: " + str(user_count) + "/" + str(len(user_ids)) + "   \r")
        sys.stdout.flush()
      user_count += 1

    print(str(len(users)) + " users queried. Total query time: " + str(timedelta(seconds=time()-start)))

    print("Processing histories with user data")
    history_count = 0
    start=time()
    for history in histories:
      uid = history['user_id']
      if uid in users.keys():
        users[uid]['histories'].append(history)
      elif uid in bad_users.keys():
        bad_users[uid]['histories'].append(history)

      h_model = db_session.query(History).filter_by(id=history['id']).first() #concurrency here
      if h_model is None:
        db_session.add(History(history))
        db_session.commit()
      else:
        h_model.update(history)
        db_session.add(h_model)
        db_session.commit()
      if history_count % 100 == 0:
        sys.stdout.write("Histories processed: " + str(history_count) + "/" + str(len(histories)) + "   \r")
        sys.stdout.flush()
      history_count += 1

    print(str(len(histories)) + " histories processed. Total time: " + str(timedelta(seconds=time()-start)))

    add_user_groups(users) # need to process bad_users groups too?
    db_session.close()

    return users, bad_users


def eligible_history(history, default_for_null=True):
  global Session
  db_session = Session()
  ret = True
  warn_threshold = datetime.now() - timedelta(days=config.EMAIL_DAYS_THRESHOLD)

  notifications = db_session.query(HistoryNotification).filter_by(h_id=history['id'], h_date=history['update_time']).all()

  if len(notifications) == 0:
    db_session.close()
    return default_for_null

  for n in notifications:
    notification = db_session.query(Notification).filter_by(id=n.n_id).first()

    if notification is not None:
      if notification.sent > warn_threshold:
        ret = False
      if notification.type == "Deletion": #always skip histories that have been notified as being deleted previously.
        print(f"ERROR: History {history['id']} already notified regarding deletion, but is presented for processing. Check past logs/db for details. Manual deletion required. Skipping.")
        ret = False

  db_session.close()
  return ret


def run(histories, dryrun=True, do_delete=False, force=False, production=False):
  global GALAXY_BASEURL
  global GALAXY_API_KEY
  global GALAXY_HIST_VIEW_BASE
  global Session
  msgs = []
  warn_users = []
  bad_users = []
  delete_users = []
  bad_delete_users = []

  warn_histories, delete_histories = filter_histories_update_time(histories, config.HISTORIES_WARN_DAYS, config.HISTORIES_DELETE_DAYS)

  msg = str(len(warn_histories)) + " histories selected for warning"
  msgs.append(msg)
  print(msg)
  process_size(warn_histories, "warnable")

  msg = str(len(delete_histories)) + " histories selected for deletion"
  msgs.append(msg)
  print(msg)
  process_size(delete_histories, "delete eligible")

  if not do_delete:
    warn_histories += delete_histories
    msg = "Not deleting histories. Delete eligible histories will be warned instead."
    msgs.append(msg)
    print(msg)

  user_ids = set()
  for history in warn_histories:
    user_ids.add(history['user_id'])

  msg=str(len(user_ids)) + " unique users for warning."
  msgs.append(msg)
  print(msg)

  warn_users, bad_users = get_users_details(user_ids, warn_histories)

  if len(bad_users) > 0:
    msg = str(len(bad_users)) + " warnable users without details. Skipping."
    msgs.append(msg)
    print(msg)

  # process warnings
  warn_weeks = int(int(config.HISTORIES_WARN_DAYS)/7)
  delete_weeks = int(int(config.HISTORIES_DELETE_DAYS)/7)
  db_session = Session()

  emailed_users = 0
  skipped_users = 0
  error_users = 0
  emailed_histories = 0
  skipped_histories = 0
  processed_users = 0
  keeplisted_users = 0
  for user in warn_users:
    if processed_users % 100 == 0:
        sys.stdout.write("Warnings processed: " + str(processed_users) + "/" + str(len(warn_users)) + "   \r")
        sys.stdout.flush()
    processed_users += 1

    keeplisted = False
    if 'groups' in warn_users[user]['details'].keys():
      for group in warn_users[user]['details']['groups']:
        if group['name'] == config.GALAXY_KEEPLIST_GROUP:
          keeplisted = True

    if keeplisted:
      keeplisted_users += 1
      continue

    try:
      username = warn_users[user]['details']['username']
    except:
      username = "Galaxy User"

    histories = []
    for i, h in enumerate(warn_users[user]['histories']):
      if force or eligible_history(h):
        del_date = datetime.now()

        first_notification = db_session.query(HistoryNotification).filter_by(h_id=h['id'], h_date=h['update_time']).first()
        if first_notification is not None:
          notification = db_session.query(Notification).filter_by(id=first_notification.n_id).first()
          if notification is None:
            ## TODO setup error check here. Really shouldn't get here unless there's manual db edits
            print("Error looking up notifcation. Defaulting to base date.")
          else:
            del_date = notification.sent
        del_date = del_date + timedelta(days=(config.HISTORIES_DELETE_DAYS-config.HISTORIES_WARN_DAYS))
        h['h_del_time'] = str(del_date.strftime('%Y-%m-%d'))
        h['h_update_time'] = str(h['update_time'].strftime('%Y-%m-%d'))
        h['h_size'] = sizeof_fmt(h['size'])
        histories.append(h)
        emailed_histories += 1
      else:
        skipped_histories += 1

    if len(histories) == 0:
      # user has no warnable histories. Skip
      skipped_users += 1
      continue

    # skip sending code if dryrun
    if dryrun:
      continue
    
    template_file = config.MAIL_TEMPLATE_WARNING
    template = Template(open(template_file).read())
    html = template.render(username = username, histories = histories, warn_weeks = warn_weeks, delete_weeks = delete_weeks, warn_period = str(config.EMAIL_DAYS_THRESHOLD), hist_view_base = GALAXY_HIST_VIEW_BASE)

    notification = Notification()
    notification.user_id = user
    notification.type = "Warning"

    # send the warning email
    try:
      email = [warn_users[user]['details']['email']]
      msg_results = send_email(to=email, html=html, subject=config.MAIL_SUBJECT_WARNING, production=production)
      notification.sent = datetime.now()
      notification.status = msg_results['status']
      if notification.status == "success":
        notification.message_id = msg_results['data']['message_id']
        message = Message()
        message.message_id = msg_results['data']['message_id']
        message.status = "Accepted"
        db_session.add(message)
        notification.message = message
        emailed_users += 1
      else:
        print("ERROR: Postal did not return as success:", msg_results)
        error_users += 1

    except:
      print("ERROR: Unable to send notification: no email for user:", user)
      notification.sent = datetime.now()
      notification.status = "Unable to send"
      error_users += 1

    db_session.add(notification)
    db_session.commit()

    waiting = True
    num_retries = 0
    failed = False
    notification_id = ""
    while waiting:
      try:
        notification_id = notification.id
        waiting = False
      except:
        print(f"Concurrency issue with database. Waiting and retrying.")
        num_retries += 1
        sleep(1)
        if num_retries > 10:
          print(f"Failed. Skipping.")
          waiting = False
          failed = True

    if failed:
      # TODO add notify here. Hope this doesn't come up
      continue

    for h in histories:
      hn = HistoryNotification()
      hn.h_id = h['id']
      hn.h_date = h['update_time']
      hn.n_id = notification_id
      db_session.add(hn)
      db_session.commit()

  msg = f"{emailed_histories} histories eligible for warning, {skipped_histories} histories skipped."
  msgs.append(msg)
  print(msg)

  msg = f"{emailed_users} users eligible for warning, {skipped_users} users skipped."
  msgs.append(msg)
  print(msg)

  if keeplisted_users > 0:
    msg = f"{keeplisted_users} users were excluded due to keeplisting."
    msgs.append(msg)
    print(msg)

  if error_users > 0:
    msg = f"{error_users} users had error sending warning notification. Check logs/db for more details."
    msgs.append(msg)
    print(msg)

  # Now handle the deletions and deletion emails if required.
  if do_delete:
    delete_user_ids = set()
    for history in delete_histories:
      delete_user_ids.add(history['user_id'])
    if len(delete_user_ids) < 1:
        msg = "No user histories require deletion."
        msgs.append(msg)
        print(msg)

        db_session.close()
        return [warn_users, bad_users, delete_users, bad_delete_users], msgs
    else:
        msg = str(len(delete_user_ids)) + " unique users for deletion of " + str(len(delete_histories)) + " histories."
        msgs.append(msg)
        print(msg)

    delete_users, bad_delete_users = get_users_details(delete_user_ids, delete_histories)

    if len(bad_delete_users) > 0:
      msg = str(len(bad_delete_users)) + " delete eligible users without details. Skipping."
      msgs.append(msg)
      print(msg)

    emailed_users = 0
    skipped_users = 0
    error_users = 0
    emailed_histories = 0
    skipped_histories = 0
    deleted_histories = 0
    error_histories = 0
    processed_users = 0
    keeplisted_users = 0
    #Craft the html template for the deletion email
    for user in delete_users:
      if processed_users % 100 == 0:
        sys.stdout.write("Deletions processed: " + str(processed_users) + "/" + str(len(delete_users)) + "   \r")
        sys.stdout.flush()
      processed_users += 1

      keeplisted = False
      if 'groups' in delete_users[user]['details'].keys():
        for group in delete_users[user]['details']['groups']:
          if group['name'] == config.GALAXY_KEEPLIST_GROUP:
            keeplisted = True

      if keeplisted:
        keeplisted_users += 1
        continue

      try:
        username = delete_users[user]['details']['username']
      except:
        username = "Galaxy User"

      histories = []
      for i, h in enumerate(delete_users[user]['histories']):
        if force or eligible_history(h, False): # requires user to have been warned about the history at least once and at least the configured days ago
          h['h_update_time'] = str(h['update_time'].strftime('%Y-%m-%d'))
          h['h_size'] = sizeof_fmt(h['size'])
          histories.append(h)
          emailed_histories += 1
        else:
          skipped_histories += 1
          # TODO once code is neater, send warning message about such histories here
          # TODO change order so that the deletion api is called first and then email sent on successful deletion

      if len(histories) == 0:
        # user has no histories. Skip
        skipped_users += 1
        continue

      # skip sending code if dryrun
      if dryrun:
        continue

      template_file = config.MAIL_TEMPLATE_DELETION
      template = Template(open(template_file).read())
      html = template.render(username = username, histories = histories, delete_weeks = delete_weeks, hist_view_base = GALAXY_HIST_VIEW_BASE)

      notification = Notification()
      notification.user_id = user
      notification.type = "Deletion"

      #send the deletion email
      try:
        email = [delete_users[user]['details']['email']]
        msg_results = send_email(to=email, html=html, subject=config.MAIL_SUBJECT_DELETION, production=production)
        notification.sent = datetime.now()
        notification.status = msg_results['status']
        if notification.status == "success":
          notification.message_id = msg_results['data']['message_id']
          message = Message()
          message.message_id = msg_results['data']['message_id']
          message.status = "Accepted"
          db_session.add(message)
          notification.message = message
          emailed_users += 1
        else:
          print("ERROR: Postal did not return as success:", msg_results)
          error_users += 1

      except:
        print("ERROR: Unable to send notification: no email for user:", user)
        notification.sent = datetime.now()
        notification.status = "Unable to send"
        error_users += 1

      db_session.add(notification)
      db_session.commit()

      waiting = True
      num_retries = 0
      failed = False
      notification_id = ""
      while waiting:
        try:
          notification_id = notification.id
          waiting = False
        except:
          print(f"Concurrency issue with database. Waiting and retrying.")
          num_retries += 1
          sleep(1)
          if num_retries > 10:
            print(f"Failed. Skipping.")
            waiting = False
            failed = True

      if failed:
        # TODO add notify here. Hope this doesn't come up
        continue

      for h in histories:
        hn = HistoryNotification()
        hn.h_id = h['id']
        hn.h_date = h['update_time']
        hn.n_id = notification_id
        db_session.add(hn)
        db_session.commit()

        #Actually do the deletion
        rem_result = remove_history(h['id'], False)
        if rem_result:
          deleted_histories += 1
        else:
          error_histories += 1
          print(f"ERROR: Unable to delete history {h['id']}")

    msg = f"{emailed_histories} histories eligible for deletion, {deleted_histories} histories deleted."
    msgs.append(msg)
    print(msg)

    msg = f"{emailed_users} users notified regarding deletion."
    msgs.append(msg)
    print(msg)

    if keeplisted_users > 0:
      msg = f"{keeplisted_users} users were excluded due to keeplisting."
      msgs.append(msg)
      print(msg)

    if error_histories > 0:
      msg = f"{error_histories} failed to be deleted. Check logs/db for more details. Manual intervention required."
      msgs.append(msg)
      print(msg)

    if skipped_histories > 0:
      msg = f"{skipped_histories} histories skipped for deletion due to no prior warning notifications, insufficient time between warning and deletion, or failed to be deleted previously. Check logs/db for more details."
      msgs.append(msg)
      print(msg)

    if skipped_users > 0:
      msg = f"{skipped_users} users skipped for notification due to having all skipped histories."
      msgs.append(msg)
      print(msg)

    if error_users > 0:
      msg = f"{error_users} users had error sending deletion notification. Check logs/db for more details."
      msgs.append(msg)
      print(msg)

  db_session.close()

  return [warn_users, bad_users, delete_users, bad_delete_users], msgs

def main(dryrun=True, production=False, do_delete=False, force=False, notify=False, drop_db=False, purge=False):
  global GALAXY_BASEURL
  global GALAXY_API_KEY
  global GALAXY_HIST_VIEW_BASE
  global db
  global Session

  if notify:
    notify_slack("Starting Galaxy History Mailer", '\n'.join([f"Dryrun: {dryrun}", "Server: " + ('Production' if production else 'Staging'), f"Deletion: {do_delete}", f"Force Notify: {force}", f"Purge: {purge}"]), 'good')

  if production:
    print("Production Galaxy server selected.")
    GALAXY_BASEURL= config.PROD_GALAXY_BASEURL
    GALAXY_API_KEY = config.PROD_GALAXY_API_KEY
    GALAXY_HIST_VIEW_BASE = config.PROD_HIST_VIEW_BASE
    db_uri = config.PROD_LOCAL_DB
  else:
    print("Staging Galaxy server selected.")
    GALAXY_BASEURL= config.STAGING_GALAXY_BASEURL
    GALAXY_API_KEY = config.STAGING_GALAXY_API_KEY
    GALAXY_HIST_VIEW_BASE = config.STAGING_HIST_VIEW_BASE
    db_uri = config.STAGING_LOCAL_DB

  engine = create_engine(db_uri)
  Session = sessionmaker(bind=engine)

  if drop_db:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("Database dropped and recreated")
    return None

  if purge:
    msgs = []
    num_deleted = 0
    num_threshold = 0
    num_previous = 0
    num_purged = 0
    num_error = 0
    hist_size = 0
    num_restored = 0
    db_session = Session()

    print("Beginning purge of previously deleted histories")
    deletion_notifications = db_session.query(Notification).filter_by(type="Deletion").all()
    warn_threshold = datetime.now() - timedelta(days=config.PURGE_DAYS_THRESHOLD)
    deletion_notification_count = 0
    deletion_notification_total = len(deletion_notifications)
    processed_histories = 0
    for deletion_notification in deletion_notifications:
      history_notifications = db_session.query(HistoryNotification).filter_by(n_id=deletion_notification.id).all()
      if deletion_notification_count % 100 == 0:
        sys.stdout.write(f"Delete notifications processsed: {deletion_notification_count}/{deletion_notification_total}  Histories purged/threshold/deleted: {num_purged}/{num_threshold}/{num_deleted}    \r")
        sys.stdout.flush()
      deletion_notification_count += 1
      for history_notification in history_notifications:
        num_deleted += 1
        if deletion_notification.sent < warn_threshold:
          history = db_session.query(History).filter_by(id=history_notification.h_id).first()
          if history:
            history_is_deleted, history_is_purged = is_history_deleted_or_purged(history)
            if history_is_deleted is None:
              print(f"Error querying /api/<history_id> for history {history.id}. No action taken")
              num_error += 1
              continue

            if history_is_deleted is False:
              # User has restored history
              history.status = "Restored"
              if not dryrun:
                db_session.add(history)
                db_session.commit()
                num_restored += 1
              print(f"History {history.id} is no longer in deleted state")

            elif history.status != "Purged":
              if history_is_purged:
                # User has purged history, or history has taken a long time to purge in a previous week,
                # resulting in 504 status from delete request
                if not dryrun:
                  history.status = "Purged"
                  db_session.add(history)
                  db_session.commit()
                num_previous += 1
                print(f"History {history.id} is already in purged state")
                continue
              num_threshold += 1
              elif not dryrun:
                rem_result = remove_history(history.id, purge=True)
                if rem_result:
                  num_purged += 1
                  hist_size += history.size
                  history.status = "Purged"
                  db_session.add(history)
                  db_session.commit()
                  print(f"Purged history: {history.id}")
                else:
                  num_error += 1
                  print(f"Unable to purge history: {history.id}")
              elif dryrun:  # dryrun option set: nothing is removed, assume everything would return 200
                print(f"Dry run. Would purge history: {history.id}")
                num_purged += 1
                hist_size += history.size
            else:
              num_previous += 1

    db_session.close()
    msgs.append(f"Deleted histories: {num_deleted}")
    msgs.append(f"Previously purged histories: {num_previous}")
    msgs.append(f"Eligible histories: {num_threshold}")
    msgs.append(f"Restored histories: {num_restored}")
    msgs.append(f"Purged histories: {num_purged}")
    msgs.append(f"Purged storage: {sizeof_fmt(hist_size)}")
    msgs.append(f"Errors: {num_error}")
    for msg in msgs:
      print(msg)
    if notify:
      notify_slack("Finished Galaxy History Mailer", '\n'.join(msgs), 'good')
    return None

  histories = get_all_histories(config.HISTORIES_WARN_DAYS)
  if histories:
    result, msgs = run(histories, dryrun=dryrun, do_delete=do_delete, force=force, production=production)
    if notify:
      notify_slack("Finished Galaxy History Mailer", '\n'.join(msgs), 'good')
    return result
  else:
    msg = "Unable to fetch histories. Quiting without any work."
    print(msg)
    if notify:
      notify_slack("Error - Galaxy Histroy Mailer", msg, 'danger')
    return None


def is_history_deleted_or_purged(history):
  """Check live status to see if history status is deleted."""
  url = (
    GALAXY_BASEURL
    + config.GALAXY_HISTORIES_EP
    + '/' + history.id  # history_table is indexed by field hid (0, 1, 2) and the hex history id is the id field
    + f'/?key={GALAXY_API_KEY}'
  )
  res = session.get(url)
  if res.status_code == 200:
    data = res.json()
    return (data["deleted"], data["purged"])
  else:
    return (None, None)


if __name__ == "__main__":
  args = argparser.parse_args()
  if not args.production and not config.STAGING_GALAXY_BASEURL:
    print("No staging URL set. Run with --production flag to use production configuration.")
  elif args.dryrun or args.warn or args.delete or args.drop_db or args.purge:
    main(dryrun=args.dryrun, production=args.production, do_delete=args.delete, force=args.force, notify=args.notify, drop_db=args.drop_db, purge=args.purge)
  else:
    print("No run type selected. Quiting without any work. Run with '--help' for usage.")
