try:
    from lxml.html import fromstring
    from lxml import etree
    from lxml.etree import LxmlError
    import lxml
except ImportError as e:
    print ("LXML missing, please install. v.3.2.4 confirmed working")
    print ("Windows binaries at http://www.lfd.uci.edu/~gohlke/pythonlibs/#lxml")
    raise e
import urllib
import urllib.request
import time
import concurrent.futures
import random
import datetime
import sys
import logging
import json
import math
import secrets
from google.transit import gtfs_realtime_pb2

logging.basicConfig(filename='gtfsScraperErrors.txt', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

# Scraper
# Downloads information about several metlink bus stops from their webpages
# Compilates them in time order, and spits out an xml document
# Uploads this to the HZ ftp server, where it can be read by the menu
#
# Changelog
# 1.3 Fixed indentation issue
#     Next-day services are properly identified by checking if
#     they occur earlier in the day than the current time (minus 10m)
# 1.2 Another change of page, to the secret Android URL.
#     Colors back
#     More error handling
#     Logs errors to busScraperErrors.txt
# 1.1 Change of page to scrape, as metlink changed the format
#     Colors no longer availible
#     Update user agents to reflect more modern browsers
# 1.0 Initial release ~2013



baseUrl = "https://api.opendata.metlink.org.nz/v1/stop-predictions?stop_id="
busStops = ['6910','5514','5515']
stopAmount = len(busStops);
sortStart = 0;
userAgents = ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36',
              'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:42.0) Gecko/20100101 Firefox/42.0',
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/601.2.7 (KHTML, like Gecko) Version/9.0.1 Safari/601.2.7',
              'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36',
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36']              

"""Takes a bus stop number, get the associated timetable
webpage and puts it in the position of the index parameter"""
def load_page(code):
    #url = baseUrl+code+"/departures"
    url = baseUrl+code
    print(url)
    #Makes a connection using the url, with a random user agent (to help avoid detection) and a timeout
    for j in range(2):
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            request = urllib.request.Request(url)
            request.add_header('X-API-KEY', secrets.api_key)
            print(request.headers)
            # Jun 22. Intermittent problem where retrieving info just seems to block. So adding explicit timeout
            response=urllib.request.urlopen(request, timeout=5)
            print("returning")
            return response.read()

        except (urllib.request.URLError,IOError) as e:
            print(e)
            eType = 'Error' if type(e) == IOError else 'Timeout'
            if(j==0):
                sys.stdout.write(eType+' fetching stop '+str(code)+' retrying in 3s\n')
                time.sleep(3)
            else:
                raise IOError(eType+' fetching stop '+str(code)+", 2 failed attempts, restarting scrape")
        
"""Makes a datetime that corresponds to the arrival of the bus,
these can be compared to find the order busses will arrive"""
def time_sort_key(busElement):
    time = busElement.attrib.get("arrives")
    if(time == 'Due'):
        return sortStart - datetime.timedelta(minutes=5)
    elif(time[-4:] == 'mins'):
        return sortStart + datetime.timedelta(minutes=int(time.split(' ',1)[0])-1,seconds=30)
    else:
        arriveTime = sortStart
        busTime = datetime.datetime.strptime(time, '%I:%M%p').time();
        #The time read in the #:##PM format will be at the default year of 19##, combine todays date with our read time
        busDateTime = datetime.datetime.combine(datetime.datetime.now(),busTime)
        #If the bus looks to arrive 5+ minutes ago, it must be tomorrow
        if(busDateTime < datetime.datetime.now() - datetime.timedelta(minutes=5)):
            arriveTime += datetime.timedelta(days=1)
        arriveTime = arriveTime.replace(hour=busTime.hour,minute=busTime.minute)
        return arriveTime

        
def generateErrorNotice():
    f = open('times.xml', 'wb')
    
    theTime = datetime.datetime.now() #Record time now for comparing to data
    errorText = '<busses scrapeDate="' + theTime.strftime("%Y:%m:%d:%H:%M:%S") + '">'
    errorText += '<bus code="" dest="Real Time Bus Info" arrives=""/>'
    errorText += '<bus code="" dest="Missing from Metlink Site" arrives=""/>'
    errorText += '</busses>'
    f.write(bytes(errorText,'UTF-8'))
    f.close()
    print('(not) Scraped at '+datetime.datetime.now().time().strftime('%I:%M:%S %p'))
    
class UploadError:

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message

#Keep updating indefinately
def scrape():
    print('Scrape begins')
#    s = time.clock()
    s = time.process_time()
    root = etree.Element('busses') #Etree is a common way to layout XML

    loadedPages = []
    #print('1')

    #Start a thread for each bus stop
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
         futures = {executor.submit(load_page,stop): stop for stop in busStops}
         #print('1.1')
         print(len(futures))
         for future in concurrent.futures.as_completed(futures):
             try:
                 print('1.2')
                 loadedPages.append(future.result())
                 print('1.2a')
             except IOError as e:
                 print('IO error - ', str(e))
                 return
             except TypeError as e:
                 print('type error - ', str(e))
                 return
             except NameError as e:
                 print('name error - ', str(e))
                 return
             except AttributeError as e:
                 print('attribute error - ', str(e))
                 return 
             except :
                 print('some other exception', sys.exc_info()[0])
                 print(future.result())
                 return 
             
    print('2')
    busses = [];
    #Process retrieved pages
    malformed = False
    goodPages = 0
    badPages = 0
    for page in loadedPages:
        try:
            thisPageGood = True
            jsonData = json.loads(page)
            #print('loaded json!')
            #print(page)		
            rowCount = len(jsonData['departures'])
            #print(rowCount)
            for row in range(rowCount):
                try:
                    #print('bus info')
                    #if (row<100):
                        #print(jsonData['departures'][row])
                        #print(str(row))
                        #print(jsonData['departures'][row]['service_id']) #[row]['service_id'])
                        #print(jsonData['departures'][row]['destination']['name'])
                    #print(jsonData['Services'][row]['DestinationStopName'])
                    bus = etree.Element('bus')
                    bus.attrib['code']= jsonData['departures'][row]['service_id']
                    bus.attrib['dest']= jsonData['departures'][row]['destination']['name']
                    
                    isRealTime = jsonData['departures'][row]['monitored']
                    currentTime = datetime.datetime.now()
                    #waitInSeconds = jsonData['departures'][row]['DisplayDepartureSeconds']
                    #print(waitInSeconds)
                    #bus.attrib['arrivalSeconds'] = str(waitInSeconds)
                    #print(jsonData['Services'][row]['DisplayDepartureSeconds'])
                    #print(arrivalMins)
                    if (isRealTime):
                        timetext = jsonData['departures'][row]['arrival']['expected']
                    else:
                        timetext = jsonData['departures'][row]['arrival']['aimed']
                    #print(timetext)
                    bustime = datetime.datetime.strptime(timetext, "%Y-%m-%dT%H:%M:%S%z")
                    waitInSeconds = bustime.timestamp() - currentTime.timestamp()
                    if (waitInSeconds < 360000):
                        #print(waitInSeconds)
                        waitInSeconds = math.trunc(waitInSeconds)
                        bus.attrib['arrivalSeconds'] = str(waitInSeconds)
                        #print(bustime)
                        tempStr = bustime.strftime('%I:%M%p').lower()
                        if (tempStr.startswith('0')):
                            tempStr = tempStr[-len(tempStr)+1:]
                        bus.attrib['arrives'] = tempStr
                        if (isRealTime):
                            bus.attrib['isRealTime']="True"
                        else:
                            bus.attrib['isRealTime']="False"
                        #bus.attrib['arrives']= jsonData['Services'][row]['DisplayDepartureSeconds']
                        #print("attrib added")
                        busses.append(bus)
                        #print("bus appended")
                except (KeyError) as e:
                    print('JSON Data not as expected, will try and move to next page')
                    #logger.exception(e)
                    badPages = badPages + 1
        except (etree.LxmlError,IndexError) as e:
            print('Page format not as expected, rescraping')
            logger.exception(e)
            malformed = True
        except (KeyError) as e:
            print('JSON Data not as expected, leaving:'+str(jsonData))
            thisPageGood = False;
            logger.exception(e)
            #malformed = True
            #return ftp
        except IndexError as e:
            return busses

        if (thisPageGood):
            goodPages = goodPages + 1
        else:
            badPages = badPages + 1
            
        if (malformed):
            print('malformed!')
            generateErrorNotice()
    
    print("out of loop, " + str(goodPages) + " good pages, " + str(badPages) + " bad pages.")
    if (not malformed):
        #print("sorting")
        # here
        busses = sorted(busses,key = lambda b: b.attrib.get("dest"))
        #print("sorted")
        global sortStart
        sortStart = datetime.datetime.now() #Record time now for comparing to data
        busses = sorted(busses,key = time_sort_key)
        #print("sorted 2")

        root.extend(busses)
        root.attrib['scrapeDate']=sortStart.strftime("%Y:%m:%d:%H:%M:%S")

        for j in range(2):
            try:    
                #Put it in a file
                f = open('times.xml', 'wb')
                f.write(etree.tostring(root, pretty_print=True))
                f.close()
                #print('Scraped at '+datetime.datetime.now().time().strftime('%I:%M:%S %p')+ " took "+str(time.clock()-s))
                print('Scraped at '+datetime.datetime.now().time().strftime('%I:%M:%S %p')+ " took "+str(time.process_time()-s))
                break
            except IOError as e:
                logger.exception(e)
                if(j==0):
                    print('Local file write failed, retrying in 2s + str(e)')
                    time.sleep(2)
                else:
                    print('Local file write failed again, re-scraping website, nothing was uploaded')
                    raise UploadError('write has failed twice, restarting scrape')

                    return
                            
    # print('uploading now')
    # for j in range(2):
    #     try:
    #         fp = open('times.xml','rb')
    #         ftp.storbinary('STOR times.xml', fp)
    #         print('Uploaded to FTP')
    #         break
    #     except IOError as e:
    #         logger.exception(e)
    #         if(j==0):
    #             print("Couldn't read file retrying in 20s" + str(e))
    #             time.sleep(20)
    #         else:
    #             print('Local file read failed again, re-scraping website, nothing was uploaded')
    #             raise UploadError('FTP has failed twice, restarting scrape')
    #             return ftp
    #     except (ConnectionError, ftplib.all_errors) as e:
    #         if(j==0):
    #             print('Upload to FTP failed, reconnecting in 3s\n'+str(e))
    #             logger.exception(e)
    #             ftp =  ftpConnect()
    #         else:
    #             print('Upload to FTP failed again, re-scraping website, nothing was uploaded\n'+str(e))
    #             logger.exception(e)
    #             return ftp
        
    
    #Wait a bit before updating (Random so it's less easy to detect)
    waitDuration = random.randint(13,19)
    print(str(waitDuration)+'s till next scrape')
    time.sleep(waitDuration)
    return


# def ftpConnect():
#     print('Connecting to ftp')
#     while(True):
#         try:
#             ftp = ftplib.FTP(ftpHost,ftpUser,ftpPass)
#             ftp.cwd(ftpFolder)
#             print('FTP connection established')
#             return ftp
#         except (ftplib.all_errors,ConnectionError) as e:
#             print('FTP connection failed, retrying in 5s\n'+str(e))
#             logger.exception(e)
#             time.sleep(5)


while(True):
    try:
        #ftp = ftpConnect()
        keepGoing = True;
        while(keepGoing):
            try:
                scrape()
                print('scrape function ended')
            # except (UploadError):
            #     print('Upload error!')
            #     keepGoing = False;
            except:
                print('some unknown exception during scrape')
            waitDuration = 20
            print(str(waitDuration)+'s till next upload')
            time.sleep(waitDuration)

    #These are legit signs we should stop
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
    #Otherwise log and keep at it
    except Exception as err:
        logger.exception(err)
                
