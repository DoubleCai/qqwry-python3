# coding=utf-8
#
# for Python 3.0+
# 来自 https://pypi.python.org/pypi/qqwry-py3
# 版本：2017-08-13
#
# 用法
# ============
# from qqwry import QQwry
# q = QQwry()
# q.load_file('qqwry.dat')
# result = q.lookup('8.8.8.8')
# 
# 
# 解释q.load_file(filename, loadindex=False)函数
# --------------
# 加载qqwry.dat文件。成功返回True，失败返回False。
# 
# 参数filename可以是qqwry.dat的文件名（str类型），也可以是bytes类型的文件内容。
# 
# 当参数loadindex=False时（默认参数）：
# ﻿程序行为：把整个文件读入内存，从中搜索
# ﻿加载速度：很快，0.004 秒
# ﻿进程内存：较少，16.9 MB
# ﻿查询速度：较慢，5.3 万次/秒
# ﻿使用建议：适合桌面程序、大中小型网站
# 
# ﻿﻿当参数loadindex=True时：
# ﻿程序行为：把整个文件读入内存。额外加载索引，把索引读入更快的数据结构
# ﻿加载速度：★★★非常慢，因为要额外加载索引，0.78 秒★★★
# ﻿进程内存：较多，22.0 MB
# ﻿查询速度：较快，18.0 万次/秒
# ﻿使用建议：仅适合高负载服务器
# 
# ﻿﻿（以上是在i3 3.6GHz, Win10, Python 3.6.2 64bit，qqwry.dat 8.86MB时的数据）
# 
# 
# 解释q.lookup('8.8.8.8')函数
# --------------
# ﻿找到则返回一个含有两个字符串的元组，如：('国家', '省份')
# ﻿没有找到结果，则返回一个None
# 
# 
# 解释q.clear()函数
# --------------
# ﻿清空已加载的qqwry.dat
# ﻿再次调用load_file时不必执行q.clear()
# 
# 
# 解释q.is_loaded()函数
# --------------
# q对象是否已加载数据，返回True或False
# 
# 
# 解释q.get_lastone()函数
# --------------
# ﻿返回最后一条数据，最后一条通常为数据的版本号
# ﻿没有数据则返回一个None

import array
import bisect
import struct
import socket

__all__ = ('QQwry',)
    
def int3(data, offset):
    return data[offset] + (data[offset+1] << 8) + \
           (data[offset+2] << 16)

def int4(data, offset):
    return data[offset] + (data[offset+1] << 8) + \
           (data[offset+2] << 16) + (data[offset+3] << 24)

class QQwry:
    dict_isp = [
        '联通',
        '移动',
        '铁通',
        '电信',
        '长城',
        '聚友',
    ]

    dict_province = [
        '北京',
        '天津',
        '重庆',
        '上海',
        '河北',
        '山西',
        '辽宁',
        '吉林',
        '黑龙江',
        '江苏',
        '浙江',
        '安徽',
        '福建',
        '江西',
        '山东',
        '河南',
        '湖北',
        '湖南',
        '广东',
        '海南',
        '四川',
        '贵州',
        '云南',
        '陕西',
        '甘肃',
        '青海',
        '台湾',
        '内蒙古',
        '广西',
        '宁夏',
        '新疆',
        '西藏',
        '香港',
        '澳门',
    ]

    dict_city_directly = [
        '北京',
        '天津',
        '重庆',
        '上海',
    ]

    def __init__(self):
        self.clear()
        
    def clear(self):
        self.idx1 = None
        self.idx2 = None
        self.idxo = None
        
        self.data = None
        self.index_begin = -1
        self.index_end = -1
        self.index_count = -1
        
        self.__fun = None
        
    def load_file(self, filename, loadindex=False):
        self.clear()
        
        if type(filename) == bytes:
            self.data = buffer = filename
            filename = 'memory data'
        elif type(filename) == str:
            # read file
            try:
                with open(filename, 'br') as f:
                    self.data = buffer = f.read()
            except Exception as e:
                print('打开、读取文件时出错：', e)
                self.clear()
                return False
            
            if self.data == None:
                print('%s load failed' % filename)
                self.clear()
                return False
        else:
            self.clear()
            return False
        
        if len(buffer) < 8:
            print('%s load failed, file only %d bytes' % 
                  (filename, len(buffer))
                  )
            self.clear()
            return False            
        
        # index range
        index_begin = int4(buffer, 0)
        index_end = int4(buffer, 4)
        if index_begin > index_end or \
           (index_end - index_begin) % 7 != 0 or \
           index_end + 7 > len(buffer):
            print('%s index error' % filename)
            self.clear()
            return False
        
        self.index_begin = index_begin
        self.index_end = index_end
        self.index_count = (index_end - index_begin) // 7 + 1
        
        if not loadindex:
            print('%s %s bytes, %d segments. without index.' %
                  (filename, format(len(buffer),','), self.index_count)
                 )
            self.__fun = self.__raw_search
            return True

        # load index
        self.idx1 = array.array('L')
        self.idx2 = array.array('L')
        self.idxo = array.array('L')
        
        try:
            for i in range(self.index_count):
                ip_begin = int4(buffer, index_begin + i*7)
                offset = int3(buffer, index_begin + i*7 + 4)
                
                # load ip_end
                ip_end = int4(buffer, offset)
                
                self.idx1.append(ip_begin)
                self.idx2.append(ip_end)
                self.idxo.append(offset+4)
        except:
            print('%s load index error' % filename)
            self.clear()
            return False

        print('%s %s bytes, %d segments. with index.' % 
              (filename, format(len(buffer),','), len(self.idx1))
               )
        self.__fun = self.__index_search
        return True
        
    def __get_addr(self, offset):
        # mode 0x01, full jump
        mode = self.data[offset]
        if mode == 1:
            offset = int3(self.data, offset+1)
            mode = self.data[offset]
        
        # country
        if mode == 2:
            off1 = int3(self.data, offset+1)
            c = self.data[off1:self.data.index(b'\x00', off1)]
            offset += 4
        else:
            c = self.data[offset:self.data.index(b'\x00', offset)]
            offset += len(c) + 1

        # province
        if self.data[offset] == 2:
            offset = int3(self.data, offset+1)
        p = self.data[offset:self.data.index(b'\x00', offset)]
        
        return c.decode('gb18030', errors='replace'), \
               p.decode('gb18030', errors='replace')
            
    def lookup(self, ip_str):
        try:
            ip = struct.unpack(">I", socket.inet_aton(ip_str))[0]
            return self.__fun(ip)
        except:
            return None

    def lookup_ex(self, ip_str):
        info = self.lookup(ip_str)

        if info is None:
            return None

        country = info[0]
        area = info[1]

        location = {
            'org_country': country,
            'org_area': area,
            'country': country,
            'area': area,
            'province': '',
            'city': '',
            'county': '',
        }

        is_china        = False
        seperator_sheng = '省'
        seperator_shi   = '市'
        seperator_xian  = '县'
        seperator_qu    = '区'

        tmp_province = country.split(seperator_sheng)
        if len(tmp_province) == 2:
            is_china = True

            location['province'] = tmp_province[0]

            if seperator_shi in tmp_province[1]:
                tmp_city = tmp_province[1].split(seperator_shi)

                location['city'] = tmp_city[0]

                if len(tmp_city) >= 2:
                    if seperator_xian in tmp_city[1]:
                        tmp_county = tmp_city[1].split(seperator_xian)
                        location['county'] = tmp_county[0] + seperator_xian

                    if len(location['county']) <= 0 and seperator_qu in tmp_city[1]:
                        tmp_qu = tmp_city[1].split(seperator_qu)
                        location['county'] = tmp_qu[0] + seperator_qu
            else:
                location['city'] = tmp_province[1]

        else:
            for province in QQwry.dict_province:
                if country.startswith(province):
                    is_china = True

                    if province in QQwry.dict_city_directly:
                        tmp_province = country.split(seperator_shi)

                        if tmp_province[0] == province:
                            location['province'] = tmp_province[0]

                            if len(tmp_province) >= 2:
                                if seperator_qu in tmp_province[1]:
                                    tmp_qu = tmp_province[1].split(seperator_qu)
                                    location['city'] = tmp_qu[0] + seperator_qu
                                elif seperator_xian in tmp_province[1]:
                                    tmp_xian = tmp_province[1].split(seperator_xian)
                                    location['county'] = tmp_xian[0] + seperator_xian

                        else:
                            location['province'] = province
                            location['org_area'] = location['org_country'] + location['org_area']
                    else:
                        location['province'] = province

                        tmp_city = country.replace(province, '')
                        if tmp_city.startswith(seperator_shi):
                            tmp_city = tmp_city[1:]

                        if seperator_shi in tmp_city:
                            tmp_city = tmp_city.split(seperator_shi)

                            location['city'] = tmp_city[0] + seperator_shi

                            if len(tmp_city) >= 2:
                                if seperator_xian in tmp_city[1]:
                                    tmp_county = tmp_city[1].split(seperator_xian)
                                    location['county'] = tmp_county + seperator_xian

                                if len(location['county']) <= 0 and seperator_qu in tmp_city[1]:
                                    tmp_qu = tmp_city[1].split(seperator_qu)
                                    location['county'] = tmp_qu[0] + seperator_qu

                    break

        if is_china:
            location['country'] = '中国'

        location['isp'] = ''
        for isp in QQwry.dict_isp:
            if isp in location['area']:
                location['isp'] = isp
                break

        return {
            'ip': ip_str,
            'country': location['country'],
            'province': location['province'],
            'city': location['city'],
            'county': location['county'],
            'isp': location['isp'],
            'area': location['country'] + location['province'] + location['city'] + location['county'] + location['org_area'],
        }
        
    def __raw_search(self, ip):
        l = 0
        r = self.index_count
        
        while r - l > 1:
            m = (l + r) // 2
            offset = self.index_begin + m * 7
            new_ip = int4(self.data, offset)
    
            if ip < new_ip:
                r = m
            else:
                l = m
        
        offset = self.index_begin + 7 * l
        ip_begin = int4(self.data, offset)
        
        offset = int3(self.data, offset+4)
        ip_end = int4(self.data, offset)
        
        if ip_begin <= ip <= ip_end:
            return self.__get_addr(offset+4)
        else:
            return None
    
    def __index_search(self, ip):
        posi = bisect.bisect_right(self.idx1, ip) - 1
        
        if posi >= 0 and self.idx1[posi] <= ip <= self.idx2[posi]:
            return self.__get_addr(self.idxo[posi])
        else:
            return None
        
    def is_loaded(self):
        return self.__fun != None
        
    def get_lastone(self):
        try:
            offset = int3(self.data, self.index_end+4)
            return self.__get_addr(offset+4)
        except:
            return None

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        fn = 'qqwry.dat'
        q = QQwry()
        q.load_file(fn)
        
        for ipstr in sys.argv[1:]:
            s = q.lookup(ipstr)
            print('%s\n%s' % (ipstr, s))
    else:
        print('请以查询ip作为参数运行')
        