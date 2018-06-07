import requests
# from requests.auth import HTTPProxyAuth
from pprint import pprint
from lxml import html

START_URL = 'https://www.sec.gov/cgi-bin/browse-edgar'
PARAM = '?CIK={}&Find=Search&owner=exclude&action=getcompany'


def download_html(url, proxy=None):
    """
    Downloads html file using random proxy from list
    :param url: string
        Url that needs to be downloaded
    :param proxy_list: list
        Proxy list containing dict{ip, port, username, password}
    :return string or None:
        HTML content of url
    """
    html_content = None
    headers = {
        "User-Agent":
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/56.0.2924.87 Safari/537.36",
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q = 0.9, image / webp, * / *;q = 0.8"
    }
    try:
        if proxy:
            http = 'http://' + proxy
            https = 'https://' + proxy
            proxies = {"http": http, "https": https}
            s = requests.Session()
            # s.trust_env = False
            s.proxies.update(proxies)
            # s.auth = HTTPProxyAuth(proxy_list[i]['username'], proxy_list[i]['password'])
            r = s.get(url, headers=headers, allow_redirects=True)
            r.raise_for_status()
        else:
            r = requests.get(url, headers=headers, allow_redirects=True)
            r.raise_for_status()
        if r.status_code != 200:
            print('[%s] Error Downloading %s' % r.status_code, url)
            print(r.headers)
            print(r.cookies)
        else:
            html_content = r.content
    except requests.exceptions.RequestException as err:
        print('Failed to open url %s' % url)
        html_content = None
    finally:
        if type(html_content) == bytes:
            return html_content.decode('utf-8')
        else:
            return html_content


def search(ticker, proxy=None):
    """
    Searches for Company Name in SEC EDGAR
    :param ticker: string
        Company ticker
    :param proxy: string
        IP address of proxy : default None
    :return:
    """
    sdict = {'CompanyTicker': ticker}
    params = PARAM.format(ticker)
    sdict['surl'] = START_URL + params
    result = download_html(sdict['surl'], proxy)
    if result:
        tree = html.fromstring(result)
        cik_a = tree.xpath('//*[@id="contentDiv"]/div[1]/div[3]/span/a/text()')
        sik_a = tree.xpath('//*[@id="contentDiv"]/div[1]/div[3]/p/a[1]/text()')
        company_name_span = tree.xpath('//*[@id="contentDiv"]/div[1]/div[3]/span/text()')
        if len(cik_a) > 0 and len(company_name_span) > 0:
            sdict['CIK'] = str(cik_a[0].split(' ')[0])
            sdict['CompanyName'] = str(company_name_span[0].strip())
            sdict['SIK'] = int(sik_a[0])
            sdict['status'] = 'OK'
        else:
            sdict['status'] = 'ERROR'
            sdict['err_info'] = 'Cannot open content'
    else:
        sdict['status'] = 'ERROR'
        sdict['err_info'] = 'Search result is empty'
    return sdict


if __name__ == '__main__':
    pprint(search('GOOG'))
