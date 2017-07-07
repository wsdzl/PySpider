# html链接解析器

from html.parser import HTMLParser
from urllib.request import urljoin
import chardet
import re

__all__ = ('AnchorParser', 'get_charset')
_re = re.compile(b'''<meta.+['"]?.*;?\s*charset=['"]?(\S+)['"]''')

def get_charset(data):
    rst = _re.findall(data)
    if rst:
        charset = rst[0].decode('ascii')
    else:
        charset = chardet.detect(data)['encoding']
    return charset

class AnchorParser(HTMLParser):

    # 初始化
    # html str/bytes 要解析的html
    # url  str       html页面url，用于合并链接，可选参数
    def __init__(self, html, url=None, charset=None):
        super().__init__()
        self.data = []
        self.url = url
        if type(html) == bytes:
            if charset is None:
                charset = 'utf-8'
            try:
                html = html.decode(charset)
            except:
                charset = get_charset(html)
                if charset:
                    try:
                        html = html.decode(charset, 'ignore')
                    except Exception as e:
                        # raise e
                        html = ''
                else:
                    html = ''
        self.html = html

    # 直接执行实例对象可获取解析结果列表
    def __call__(self):
        self.feed(self.html)
        return self.data

    def handle_starttag(self, tag, attrs):
        if tag != 'a': # 仅解析a标签
            return
        for attr in attrs:
            if attr[0] == 'href':
                link = attr[1]
                if link.startswith('mailto:') or link.startswith('javascript:'):
                    return # 跳过mailto和javascript链接
                seek = link.find('#')
                if seek != -1:
                    link = link[:seek-len(link)] # 去掉锚链接
                if link: # 过滤空链接
                    if self.url:
                        link = urljoin(self.url, link) # 合并到html页面url
                    while link.endswith('/'):
                        link = link[:-1]
                    self.data.append(link)

if __name__ == '__main__':
    url = 'http://www.baidu.com'
    from urllib.request import urlopen as uo
    parser = AnchorParser(uo(url).read(), url)
    print(parser())