# -*- coding: utf-8 -*-
"""
search youtube videos with keywords search
could use separately or by calling function
YouTubeSearcher integrates the search request construction and result parsing functions, a direct way is to use `search_iter` to traverse the results returned by keywords
"""

import json
import argparse
import requests
from urllib.parse import urlencode
from traceback import print_exc
from enum import Enum
from log_settings import get_logger


logger = get_logger("searcher")

# updated on 2020.03.09
SEARCH_API = 'https://www.youtube.com/youtubei/v1/search'
INNER_API_KEY = 'AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8'
CLIENT_VERSION = '2.20210307.08.01'
USER_AGENT = "Mozilla/5.0 (Mac) Gecko/20100101 Firefox/76.0"
SUBTITLE_ARG = "EgIoAQ=="


# program settings
class Fmt(str, Enum):
    origin = 'origin'
    only_id = 'only_id'
    url = 'url'
    url_title = 'url_title'


def get_first_item_with_member(a, member):
    """
    Get the first item from 'a' which has an element named 'member'
    """
    if a is None:
        return
    for item in a:
        if member in item:
            return item


def getitem(d, *path):
    """
    Traverse a nested python object, path items select which object is selected:
     * a tuple: selects a dictionary from a list which contains the specified key
     * an integer: select the specified item from a list.
     * a string: select the specified item from a dictionary.
    """
    for k in path:
        if type(k) == tuple:
            d = get_first_item_with_member(d, *k)
        elif type(k) == int:
            d = d[k]
        else:
            d = d.get(k)

        if d is None:
            return
    return d


def get_search_results(js, fmt=Fmt.url_title):
        # format methods
        def only_id_format(l):
            tmp = []
            for i in l:
                _id = getitem(i, "videoRenderer", "videoId")
                if _id is None or not _id:
                    continue
                tmp.append({"id": _id})
            return tmp

        def gen_url_format(l):
            tmp = []
            for i in l:
                _id = getitem(i, "videoRenderer", "videoId")
                if _id is None or not _id:
                    continue
                url = 'https://www.youtube.com/watch?v={}'.format(_id)
                tmp.append({"url": url, "id": _id})
            return tmp

        def gen_url_with_title(l):
            tmp = []
            for i in l:
                _id = getitem(i, "videoRenderer", "videoId")
                if _id is None or not _id:
                    continue
                url = 'https://www.youtube.com/watch?v={}'.format(_id)
                title = getitem(i, "videoRenderer", "title", "runs", 0, "text")
                tmp.append({"url": url, "title": title, "id": _id})
            return tmp

        # ====================== main process ===========================
        if not js:
            return [], None
        ct = getitem(js, "contents", "twoColumnSearchResultsRenderer", "primaryContents", "sectionListRenderer", "contents")
        if not ct:
            ct = getitem(js, "onResponseReceivedCommands", 0, "appendContinuationItemsAction", "continuationItems")

        result_list = getitem(ct, ("itemSectionRenderer",), "itemSectionRenderer", "contents")
        cont = getitem(ct, ("continuationItemRenderer",), "continuationItemRenderer", "continuationEndpoint", "continuationCommand", "token")

        if fmt == Fmt.origin:
            ret_list = result_list
        elif fmt == Fmt.only_id:
            ret_list = only_id_format(result_list)
        elif fmt == Fmt.url:
            ret_list = gen_url_format(result_list)
        elif fmt == Fmt.url_title:
            ret_list = gen_url_with_title(result_list)
        else:
            ret_list = result_list
        if ret_list is None:
            ret_list = []
        return ret_list, cont


class YouTubeSearcher(object):
    def __init__(self):
        self.session = requests.Session()

    def req_post(self, url, data=None):
        """
        POST request to src.
        """
        hdrs = {
            "x-src-client-name": "1",
            "x-src-client-version": CLIENT_VERSION,
            "User-Agent": USER_AGENT
        }

        try:
            resp = self.session.post(url, json=data, headers=hdrs, verify=False)
            return resp.json()
        except Exception as e:
            print_exc()
            return None

    def search(self, query_str, cont, subtitle_filter=True):
        """
        Returns next batch of search results
        """
        query = {
            "key": INNER_API_KEY
        }
        post_data = {
            "context": {"client": {"clientName": "WEB", "clientVersion": CLIENT_VERSION}},
            "query": query_str,
        }
        if cont:
            post_data["continuation"] = cont
        if subtitle_filter:
            post_data["params"] = SUBTITLE_ARG
        return self.req_post(SEARCH_API + "?" + urlencode(query), post_data)

    def search_iter(self, query_words, fmt=Fmt.url_title, subtitle_filter=True, show=True, page_total=0):
        search_list = []
        last_cont = cont = ''
        page_cnt = 0
        while True:
            try:
                if page_total and page_cnt >= page_total:
                    break
                last_cont = cont
                page = self.search(query_words, cont, subtitle_filter)
                search_list, cont = get_search_results(page, fmt)
                page_cnt += 1
                if cont is None or last_cont == cont:
                    break
                logger.debug(len(search_list))
                while len(search_list) > 0:
                    ret = search_list.pop()
                    if show:
                        logger.info(ret)
                    yield ret
            except Exception as e:
                print_exc()
                logger.warning("TODO: robust tests")
                search_list.clear()
                continue


def search_api(client, query_words, min_num=1, fmt='origin', subtitle_filter=True,
               dump_path="youtube_search_result.json", show=True):
    search_list = []
    cont = None
    while min_num > len(search_list):
        try:
            page = client.search(query_words, cont, subtitle_filter)
            res_list, cont = get_search_results(page, fmt)
            if show:
                for i in res_list:
                    logger.info(i)
            search_list.extend(res_list)
        except Exception as e:
            print_exc()
            logger.warning("TODO: robust tests")
            break
    with open(dump_path, 'w') as f:
        f.write('\n'.join([json.dumps(i) for i in search_list]))
    return search_list


def parse_search_args():
    def str2bool(v):
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Unsupported value encountered.')

    parser = argparse.ArgumentParser(description='Youtube Search videos')
    parser.add_argument('--subtitles-filter', type=str2bool, default=True, help='subtitles filter switch, default True')
    parser.add_argument('--query', '-q', type=str, help='query string')
    parser.add_argument('--minimum-number', '-n', type=int, default=10, help='fetch minimum number of videos')
    parser.add_argument('--format', type=str, help='can be: '+'; '.join([v.value for v in Fmt]))
    parser.add_argument('--output-path', '-o', type=str, help='output dump file')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_search_args()
    ys = YouTubeSearcher()
    res = search_api(ys,
                     query_words=args.query,
                     min_num=args.minimum_number,
                     fmt=args.format,
                     subtitle_filter=args.subtitles_filter,
                     dump_path=args.output_path,
                     show=True)
