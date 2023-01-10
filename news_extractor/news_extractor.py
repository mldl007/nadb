import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests as r
import regex as re
from dateutil import parser


def date_time_parser(dt):
    """
    Computes the minutes elapsed since published time.
    :param dt: date
    :return: int, minutes elapsed.
    """
    return int(np.round((dt.now(dt.tz) - dt).total_seconds() / 60, 0))

def text_clean(desc):
    """
    Cleans the text by removing special chars.
    :param desc: string containing description
    :return: str, cleaned description.
    """
    desc = desc.replace("&lt;", "<")
    desc = desc.replace("&gt;", ">")
    desc = re.sub("<.*?>", "", desc)
    desc = desc.replace("#39;", "'")
    desc = desc.replace('&quot;', '"')
    desc = desc.replace('&nbsp;', ' ')
    desc = desc.replace('#32;', ' ')
    return desc


def rss_parser(i):
    """
    Returns a data frame of parsed news item.
    :param i: single news item in RSS feed.
    :return: Data frame of parsed news item.
    """
    b1 = BeautifulSoup(str(i), "xml")
    title = "" if b1.find("title") is None else b1.find("title").get_text()
    title = text_clean(title)
    url = "" if b1.find("link") is None else b1.find("link").get_text()
    desc = "" if b1.find("description") is None else b1.find("description").get_text()
    desc = text_clean(desc)
    desc = f'{desc[:300]}...' if len(desc) >= 300 else desc
    date = "Sat, 12 Aug 2000 13:39:15 +0530" if b1.find("pubDate") is None else b1.find("pubDate").get_text()
    if url.find("businesstoday.in") >= 0:
        date = date.replace("GMT", "+0530")
    date1 = parser.parse(date)
    return pd.DataFrame({"title": title,
                         "url": url,
                         "description": desc,
                         "parsed_date": date1}, index=[0])


def src_parse(rss):
    """
    Returns the root domain name (eg. livemint.com is extracted from www.livemint.com
    :param rss: RSS URL
    :return: str, string containing the source name
    """
    if rss.find('ndtvprofit') >= 0:
        rss = 'ndtv profit'
    rss = rss.replace("https://www.", "")
    rss = rss.split("/")
    return rss[0]


def news_agg(rss):
    """
    Returns feeds from each 'rss' URL.
    :param rss: RSS URL.
    :return: Data frame of processed articles.
    """
    try:
        rss_df = pd.DataFrame()
        resp = r.get(rss, headers={
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                          "(KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"})
        b = BeautifulSoup(resp.content, "xml")
        items = b.find_all("item")
        for i in items:
            rss_df = rss_df.append(rss_parser(i)).copy()
        rss_df["description"] = rss_df["description"].replace([" NULL", ''], np.nan)
        rss_df.dropna(inplace=True)
        rss_df["src"] = src_parse(rss)
        rss_df["elapsed_time"] = rss_df["parsed_date"].apply(date_time_parser)
        rss_df["parsed_date"] = rss_df["parsed_date"].astype("str")
        # rss_df["elapsed_time_str"] = rss_df["elapsed_time"].apply(elapsed_time_str)
    except Exception as e:
        print(e)
        pass
    return rss_df


# List of RSS feeds
rss = ['https://www.economictimes.indiatimes.com/rssfeedstopstories.cms',
       'http://feeds.feedburner.com/ndtvprofit-latest?format=xml',
       'https://www.thehindubusinessline.com/news/feeder/default.rss',
       'https://www.moneycontrol.com/rss/latestnews.xml',
       'https://www.livemint.com/rss/news',
       'https://www.business-standard.com/rss/latest.rss',
       'https://www.businesstoday.in/rssfeeds/?id=225346',
       'https://www.zeebiz.com/latest.xml/feed',
       'https://www.timesofindia.indiatimes.com/rssfeedmostrecent.cms']


def get_news():
    final_df = pd.DataFrame()
    for i in rss:
        final_df = final_df.append(news_agg(i))

    final_df.sort_values(by="elapsed_time", inplace=True)
    # final_df['src_time'] = final_df['src'] + ("&nbsp;" * 5) + final_df["elapsed_time_str"]
    # final_df.drop(columns=['date', 'parsed_date', 'src', 'elapsed_time', 'elapsed_time_str'], inplace=True)
    final_df.drop(columns=['elapsed_time'], inplace=True)
    final_df.drop_duplicates(subset='description', inplace=True)
    final_df = final_df.loc[(final_df["title"] != ""), :].copy()
    return final_df
