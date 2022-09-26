#Название вакансии
#Требуемый опыт работы
#Заработную плату
#Регион

from time import sleep
from types import NoneType
import lxml

import requests as req
import json
from bs4 import BeautifulSoup

Url = "https://hh.ru/search/vacancy?text=Python+разработчик&from=suggest_post&area=1"
header = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.42"}
resp = req.get(Url,headers = header)
soup = BeautifulSoup(resp.text,"lxml");

tags = soup.find_all(attrs={"class":"bloko-header-section-3"})
for item  in tags:
    name = item.find(attrs={"target":"_blank"})
    
    if type(name) != NoneType:   # = item.find(attrs={"target":"_blank"})
        NameVacancy = name.text
        url_object = name.get('href')
        sleep(2)
        resp_object = req.get(url_object, headers= header)
        soup_object = BeautifulSoup(resp_object.text,"lxml")
        all_content = soup_object.find(attrs={"name":"description"}).get('content')
        All_info = all_content.split(".")
        Salary = All_info[1]
        Experience = All_info[4]
        Region = All_info[3]
        print(NameVacancy, Experience,Salary, Region )
