#Название вакансии
#Требуемый опыт работы
#Заработную плату
#Регион

#from asyncore import file_dispatcher
#from dbm import dumb
from time import sleep
from types import NoneType
import lxml

import requests as req
import json
from bs4 import BeautifulSoup
data = { "data":[]}

Url = "https://hh.ru/search/vacancy?text=Python+разработчик&from=suggest_post&area=113"
header = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.42"}
resp = req.get(Url,headers = header)
soup = BeautifulSoup(resp.text,"lxml");
tagPagers = soup.find_all(attrs={"class":"bloko-button"})
for item in tagPagers:
    Pager = item.text
    if Pager != None and Pager.isnumeric():
        lastPage = Pager
for page in range(1,3): #int(lastPage)+2):
    print(page)
    url_page = f"https://hh.ru/search/vacancy?text=Python+%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%87%D0%B8%D0%BA&from=suggest_post&area=113&page={page-1}&hhtmFrom=vacancy_search_list"
    resp_page = req.get(url_page,headers = header)
    soup_page = BeautifulSoup(resp_page.text,"lxml");
    tags = soup_page.find_all(string={"serp-item__title"})
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
            data["data"].append({"title":NameVacancy , "work experience":Experience,  "salary":Salary,  "region":Region })
            print(NameVacancy, Experience,Salary, Region )

with open("data.json","w") as file:
    json.dump(data, file)
