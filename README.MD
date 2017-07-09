# PySpider

Python多线程网站爬虫

- 使用线程池机制，并发爬取链接
- 使用SQLite数据库保存爬行数据
- 可自定义爬取深度、关键词等参数
- 可下载整个网站资源到本地文件夹
- 自动记录日志

## 参数说明

```
Usage: spider.py [-u url] [-d deep] [-f logfile] [-l loglevel(1-5)]
                 [--thread number] [--dbfile filepath] [--key="<keyword>"]
                 [--pridomain/-p] [--download/-D] [--testself]
Options: 
    -h, --help         查看帮助信息。
    -u url             指定爬虫开始地址，必选参数。
    -d deep            指定爬虫深度，可选参数，默认为7。
    -f logfile         保存日志到指定文件，可选参数，默认为spider.log。
    -l loglevel(1-5)   日志记录文件记录详细程度，数字越大记录越详细，可选参数，默认为5。
    --thread number    指定线程池大小，多线程爬取页面，可选参数，默认为10。
    --dbfile filepath  存放结果数据到指定的数据库（sqlite）文件中，可选参数，默认为data.db。
    --key="<keyword>"  页面内的关键词，获取满足该关键词的网页，可选参数，默认为所有页面。
    --pridomain/-p     仅爬行主域名，可选参数，默认爬行主域名及所有子域名链接。
    --download/-D      下载网站所有资源到本地文件夹，可选参数。
    --testself         程序自测，可选参数。
```

## 功能演示

![1](https://raw.githubusercontent.com/wsdzl/PySpider/master/imgs/1.jpg)
![2](https://raw.githubusercontent.com/wsdzl/PySpider/master/imgs/2.jpg)
![3](https://raw.githubusercontent.com/wsdzl/PySpider/master/imgs/3.jpg)
![4](https://raw.githubusercontent.com/wsdzl/PySpider/master/imgs/4.jpg)
![5](https://raw.githubusercontent.com/wsdzl/PySpider/master/imgs/5.jpg)
![6](https://raw.githubusercontent.com/wsdzl/PySpider/master/imgs/6.jpg)