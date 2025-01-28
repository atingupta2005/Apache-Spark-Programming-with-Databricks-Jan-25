# Databricks notebook source
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql import Row

# COMMAND ----------

import datetime
courses = [
    {
        'course_id': 1,
        'course_title': 'Mastering Python',
        'course_published_dt': datetime.date(2021, 1, 14),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 2, 18, 16, 57, 25)
    },
    {
        'course_id': 2,
        'course_title': 'Data Engineering Essentials',
        'course_published_dt': datetime.date(2021, 2, 10),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 3, 5, 12, 7, 33)
    },
    {
        'course_id': 3,
        'course_title': 'Mastering Pyspark',
        'course_published_dt': datetime.date(2021, 1, 7),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 4, 6, 10, 5, 42)
    },
    {
        'course_id': 4,
        'course_title': 'AWS Essentials',
        'course_published_dt': datetime.date(2021, 3, 19),
        'is_active': False,
        'last_updated_ts': datetime.datetime(2021, 4, 10, 2, 25, 36)
    },
    {
        'course_id': 5,
        'course_title': 'Docker 101',
        'course_published_dt': datetime.date(2021, 2, 28),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 3, 21, 7, 18, 52)
    }
]

courses_df = spark.createDataFrame([Row(**course) for course in courses])

# COMMAND ----------

users = [{
  "user_id": 1,
  "user_first_name": "Sandra",
  "user_last_name": "Karpov",
  "user_email": "skarpov0@ovh.net"
}, {
  "user_id": 2,
  "user_first_name": "Kari",
  "user_last_name": "Dearth",
  "user_email": "kdearth1@so-net.ne.jp"
}, {
  "user_id": 3,
  "user_first_name": "Joanna",
  "user_last_name": "Spennock",
  "user_email": "jspennock2@redcross.org"
}, {
  "user_id": 4,
  "user_first_name": "Hirsch",
  "user_last_name": "Conaboy",
  "user_email": "hconaboy3@barnesandnoble.com"
}, {
  "user_id": 5,
  "user_first_name": "Loreen",
  "user_last_name": "Malin",
  "user_email": "lmalin4@independent.co.uk"
}, {
  "user_id": 6,
  "user_first_name": "Augy",
  "user_last_name": "Christon",
  "user_email": "achriston5@mlb.com"
}, {
  "user_id": 7,
  "user_first_name": "Trudey",
  "user_last_name": "Choupin",
  "user_email": "tchoupin6@de.vu"
}, {
  "user_id": 8,
  "user_first_name": "Nadine",
  "user_last_name": "Grimsdell",
  "user_email": "ngrimsdell7@sohu.com"
}, {
  "user_id": 9,
  "user_first_name": "Vassily",
  "user_last_name": "Tamas",
  "user_email": "vtamas8@businessweek.com"
}, {
  "user_id": 10,
  "user_first_name": "Wells",
  "user_last_name": "Simpkins",
  "user_email": "wsimpkins9@amazon.co.uk"
}]

users_df = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

course_enrolments = [{
  "course_enrolment_id": 1,
  "user_id": 10,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 2,
  "user_id": 5,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 3,
  "user_id": 7,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 4,
  "user_id": 9,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 5,
  "user_id": 8,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 6,
  "user_id": 5,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 7,
  "user_id": 4,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 8,
  "user_id": 7,
  "course_id": 3,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 9,
  "user_id": 8,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 10,
  "user_id": 3,
  "course_id": 3,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 11,
  "user_id": 7,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 12,
  "user_id": 3,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 13,
  "user_id": 5,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 14,
  "user_id": 4,
  "course_id": 3,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 15,
  "user_id": 8,
  "course_id": 2,
  "price_paid": 9.99
}]

course_enrolments_df = spark.createDataFrame([Row(**ce) for ce in course_enrolments])

# COMMAND ----------

courses_df.show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

course_enrolments_df.show()

# COMMAND ----------



users1 = [
    {
        "email":"alovett0@nsw.gov.au",
        "first_name":"Aundrea",
        "last_name":"Lovett",
        "gender":"Male",
        "ip_address":"62.72.1.143"
    },
    {
        "email":"bjowling1@spiegel.de",
        "first_name":"Bettine",
        "last_name":"Jowling",
        "gender":"Female",
        "ip_address":"26.250.197.47"
    },
    {
        "email":"rablitt2@technorati.com",
        "first_name":"Reggie",
        "last_name":"Ablitt",
        "gender":"Male",
        "ip_address":"104.181.218.238"
    },
    {
        "email":"tgavahan3@printfriendly.com",
        "first_name":"Ted",
        "last_name":"Gavahan",
        "gender":"Female",
        "ip_address":"216.80.86.100"
    },
    {
        "email":"ccastellan4@bloglovin.com",
        "first_name":"Chantal",
        "last_name":"Castellan",
        "gender":"Female",
        "ip_address":"178.93.82.145"
    },
    {
        "email":"hcurrier5@hexun.com",
        "first_name":"Herrick",
        "last_name":"Currier",
        "gender":"Male",
        "ip_address":"98.120.5.78"
    },
    {
        "email":"zlendrem6@columbia.edu",
        "first_name":"Zorina",
        "last_name":"Lendrem",
        "gender":"Female",
        "ip_address":"219.128.213.53"
    },
    {
        "email":"lbutland7@time.com",
        "first_name":"Lilas",
        "last_name":"Butland",
        "gender":"Female",
        "ip_address":"109.110.131.151"
    },
    {
        "email":"palfonsetti8@ask.com",
        "first_name":"Putnam",
        "last_name":"Alfonsetti",
        "gender":"Female",
        "ip_address":"167.97.48.246"
    },
    {
        "email":"hunitt9@bizjournals.com",
        "first_name":"Holden",
        "last_name":"Unitt",
        "gender":"Female",
        "ip_address":"142.228.161.192"
    },
    {
        "email":"dmcmorrana@reference.com",
        "first_name":"Dorice",
        "last_name":"McMorran",
        "gender":"Female",
        "ip_address":"233.1.28.220"
    },
    {
        "email":"afaulconerb@barnesandnoble.com",
        "first_name":"Andris",
        "last_name":"Faulconer",
        "gender":"Female",
        "ip_address":"109.40.175.103"
    },
    {
        "email":"kupexc@sun.com",
        "first_name":"Krispin",
        "last_name":"Upex",
        "gender":"Male",
        "ip_address":"154.110.22.75"
    },
    {
        "email":"fmancktelowd@youku.com",
        "first_name":"Farand",
        "last_name":"Mancktelow",
        "gender":"Genderqueer",
        "ip_address":"190.20.187.10"
    },
    {
        "email":"kdodgshune@google.com",
        "first_name":"Kellyann",
        "last_name":"Dodgshun",
        "gender":"Female",
        "ip_address":"80.247.105.228"
    }
]

users1_df = spark.createDataFrame([Row(**user) for user in users1])


users2 = [{
        "email":"lbutland7@time.com",
        "first_name":"Lilas",
        "last_name":"Butland",
        "gender":"Female",
        "ip_address":"109.110.131.151"
    },
    {
        "email":"palfonsetti8@ask.com",
        "first_name":"Putnam",
        "last_name":"Alfonsetti",
        "gender":"Female",
        "ip_address":"167.97.48.246"
    },
    {
        "email":"hunitt9@bizjournals.com",
        "first_name":"Holden",
        "last_name":"Unitt",
        "gender":"Female",
        "ip_address":"142.228.161.192"
    },
    {
        "email":"dmcmorrana@reference.com",
        "first_name":"Dorice",
        "last_name":"McMorran",
        "gender":"Female",
        "ip_address":"233.1.28.220"
    },
    {
        "email":"afaulconerb@barnesandnoble.com",
        "first_name":"Andris",
        "last_name":"Faulconer",
        "gender":"Female",
        "ip_address":"109.40.175.103"
    },
    {
        "email":"kupexc@sun.com",
        "first_name":"Krispin",
        "last_name":"Upex",
        "gender":"Male",
        "ip_address":"154.110.22.75"
    },
    {
        "email":"fmancktelowd@youku.com",
        "first_name":"Farand",
        "last_name":"Mancktelow",
        "gender":"Genderqueer",
        "ip_address":"190.20.187.10"
    },
    {
        "email":"kdodgshune@google.com",
        "first_name":"Kellyann",
        "last_name":"Dodgshun",
        "gender":"Female",
        "ip_address":"80.247.105.228"
    },
    {
        "email":"kbaressf@geocities.jp",
        "first_name":"Karly",
        "last_name":"Baress",
        "gender":"Female",
        "ip_address":"145.232.153.145"
    },
    {
        "email":"amillinsg@com.com",
        "first_name":"Adelaide",
        "last_name":"Millins",
        "gender":"Female",
        "ip_address":"75.160.220.182"
    },
    {
        "email":"skemsleyh@quantcast.com",
        "first_name":"Shir",
        "last_name":"Kemsley",
        "gender":"Male",
        "ip_address":"234.195.73.177"
    },
    {
        "email":"kchomiszewskii@simplemachines.org",
        "first_name":"Kristo",
        "last_name":"Chomiszewski",
        "gender":"Female",
        "ip_address":"60.91.73.198"
    },
    {
        "email":"rkelwickj@baidu.com",
        "first_name":"Rosemonde",
        "last_name":"Kelwick",
        "gender":"Genderfluid",
        "ip_address":"42.50.134.65"
    }
]

users2_df = spark.createDataFrame([Row(**user) for user in users2])