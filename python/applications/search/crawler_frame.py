import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
import requests
import urllib2
from urlparse import urlparse, urljoin

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

class Analytics:
    urlCount = 0
    maxOutPutUrlCount = 0
    maxOutPutUrlList = list()
    isDone = False
    invalidUrlCount = 0
    subDomainDic = {}

analytics = Analytics()

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "37082069_20809476_60407382"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 Grad 37082069,20809476,60407382"
		
        self.frame = frame

        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]

            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

            #write out the analytics result to disk
            outputFile = open('Analytics.txt', 'w')
            outputFile.write('Received invalid links from frontier:' + str(analytics.invalidUrlCount) + '\n')

            outputFile.write('Pages with the most out links are:')
            for url in analytics.maxOutPutUrlList:
                outputFile.write(url + ',')

            outputFile.write(', links count is:' + str(analytics.maxOutPutUrlCount) + '\n')

            for key in analytics.subDomainDic:
                outputFile.write('Received subdomains:' + key)
                outputFile.write(', number of urls it has:' + str(analytics.subDomainDic.get(key)) + '\n')

            outputFile.close()

    def shutdown(self):
        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):

    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    #print rawDatas
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''

def extract_next_links(rawDatas):
    outputLinks = list()
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''
    if analytics.isDone:
        return outputLinks

    for rawData in rawDatas:
        if rawData.is_redirected:
            rootUrl = rawData.final_url
        else:
            rootUrl = rawData.url

        # print"------------------------------------------------"
        # print rawData.headers
        # print rawData.headers == {}
        # print rawData.error_message
        # print rawData.error_message != ''
        # print"-------------------------------------------------"

        #check if the url is valid or has any error message, if invalid, increase invalidUrlCount and continue
        if rawData.headers == {} or rawData.error_message != '' or rawData.bad_url:
            analytics.invalidUrlCount += 1
            continue

        # page = requests.get(rootUrl)
        tempCount = 0
        page = rawData.content

        # Unicode
        # doc = page.decode('gb2312', 'ignore')

        htmlParse = html.document_fromstring(page)
        htmlParse.make_links_absolute(rootUrl)  # This makes all links in the document absolute, rootUrl is the "base_href".

        # Introduction about "iterlinks()"
        # This finds any link in an action, archive, background, cite, classid, codebase, data, href, longdesc, profile, src, usemap, dynsrc, or lowsrc attribute.
        # It also searches style attributes for url(link), and <style> tags for @import and url().
        for element, attribute, link, pos in htmlParse.iterlinks():
            if link != rootUrl and is_valid(link):

                hostname = urlparse(link).hostname
                subDomain = hostname[4:len(hostname)]
                analytics.subDomainDic[subDomain] = analytics.subDomainDic.get(subDomain, 0) + 1

                outputLinks.append(link)
                tempCount += 1

            else:
                if is_valid(link):
                    rawData.bad_url = True
                analytics.invalidUrlCount += 1


        if (analytics.maxOutPutUrlCount < tempCount):
            analytics.maxOutPutUrlCount = tempCount
            analytics.maxOutPutUrlList.append(link)

    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''

    #filter timeout urls
    # try:
    #     content = urllib2.urlopen(url, timeout=15)
    # except:
    #     return False

    # url = content.get(url)
    parsed = urlparse(url)
    #print url

    if parsed.scheme not in set(["http", "https"]):
        return False
    try:

        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())\
            and not re.match(".*calendar\.ics\.uci\.edu.*"
                             + "|.*ngs\.ics\.uci\.edu.*"
                             + "|.*ganglia\.ics\.uci\.edu.*"
                             + "|.*intranet\.ics\.uci\.edu.*"
                             + "|.*graphmod\.ics\.uci\.edu.*", parsed.netloc.lower()) \
            and not "/" in parsed.query \
            and not parsed.path.count(".php") > 1 \
            and not parsed.path.count(".html") > 1 \
            and not ".php/" in parsed.path \
            and not ".html/" in parsed.path


    except TypeError:
        print ("TypeError for ", parsed)



