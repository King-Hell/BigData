#!/bin/python3
"""KWIC数据抽象方式解决方案"""


class Text:
    """文本类"""

    def setChar(self, char):
        """设置关键字数组"""
        self.__char = char

    def getChar(self):
        """获取关键字数组"""
        return self.__char


class Index:
    """索引类"""

    def setChar(self, char):
        """设置关键字数组"""
        self.__char = char

    def setup(self):
        """产生关键字索引"""
        lineList = []
        for i in char:
            wordList = i.split()
            lineList.append(wordList.copy())
            l = len(wordList)
            for j in range(l - 1):
                last = wordList.pop()
                wordList.insert(0, last)
                lineList.append(wordList.copy())
        self.__char = lineList

    def getChar(self):
        """获取关键字数组"""
        return self.__char


class AlphaIndex:
    """字母顺序排序索引类"""

    def setChar(self, char):
        """产生关键字索引"""
        self.__char = char

    def alph(self):
        """按字母顺序排序索引"""
        self.__char = sorted(self.__char, key=lambda line: line[0])

    def ith(self):
        """生成排序索引迭代器"""
        for i in self.__char:
            yield i


if __name__ == '__main__':
    """主控制器"""
    with open('input.txt') as input_file:
        # 读入输入文件
        char = input_file.readlines()
        # 文本模块
        text = Text()
        text.setChar(char)
        # 循环位移模块
        index = Index()
        index.setChar(text.getChar())
        index.setup()
        # 按字母顺序排序模块
        alphaIndex = AlphaIndex()
        alphaIndex.setChar(index.getChar())
        alphaIndex.alph()
        # 输出
        for i in alphaIndex.ith():
            print(i)
