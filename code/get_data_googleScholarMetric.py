import requests
import re

import sys
import bs4


head = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 Safari/537.36'}

# test = 'http://jp.tingroom.com/yuedu/yd300p/'
html_address = 'https://scholar.google.com/citations?hl=en&view_op=search_venues&vq=Computer+Vision+and+Pattern+Recognition&btnG='
html = requests.get(html_address, headers=head)

html.encoding = 'utf-8'  # 这一行是将编码转为utf-8否则中文会显示乱码。
# print(html.text)
print('Link: ', html_address)
print()

# Following we decrypt the link.
soup = bs4.BeautifulSoup(str(html.text), "lxml")

# If we want to find out h-index results in multiple lines. Then we can use following code to deal with each line.
# First Line result
fistLine_result = soup.find_all(name='div',attrs={"class":"gsc_mvt_table_wrapper"})[0].find_all('table')[0].findAll('tr')[1]
# print(fistLine_result)
venue_name_firstLine = fistLine_result.find_all("td")[1].text
print('venue_name_firstLine: ', venue_name_firstLine)
h5_index_fistLine = fistLine_result.find_all("td")[2].text
print('h5_index_fistLine: ', h5_index_fistLine)
h5_median_fistLine = fistLine_result.find_all("td")[3].text
print('h5_median_fistLine: ', h5_median_fistLine)
print()


# Second Line result
secondLine_result = soup.find_all(name='div',attrs={"class":"gsc_mvt_table_wrapper"})[0].find_all('table')[0].findAll('tr')[2]
# print(secondLine_result)
venue_name_secondLine = secondLine_result.find_all("td")[1].text
print('venue_name_secondLine: ', venue_name_secondLine)
h5_index_secondLine = secondLine_result.find_all("td")[2].text
print('h5_index_secondLine: ', h5_index_secondLine)
h5_median_secondLine = secondLine_result.find_all("td")[3].text
print('h5_median_secondLine: ', h5_median_secondLine)
print()



# h5_index = soup.find_all(name='a',attrs={"class":"gs_ibl gsc_mp_anchor"})[0].text # return the first line that match.
# h5_index = soup.find_all(name='td',attrs={"class":"gsc_mvt_n"})[0].find_all(name='a',attrs={"class":"gs_ibl gsc_mp_anchor"})[0].text # return the first line that match.
# print(h5_index)
# h5_median =





