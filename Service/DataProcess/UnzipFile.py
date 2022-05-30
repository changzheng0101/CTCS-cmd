# -*- coding: utf-8 -*-
"""
@File    :  UnzipFile.py
@Time    :  2022/4/3 10:05
@Author  :  zhuyugang
@Version :  1.0
@Desc    :  主界面的AI准确度窗口
"""

import os
import zlib
import unrar
import shutil
import zipfile
import tarfile
from pathlib import Path
from time import sleep
import pandas as pd
from unrar import rarfile

filepath = "../data "  # relative path


class BaseTool(object):
    def __init__(self, mode):
        self.mode = mode
        self.compress = [".tar.gz", ".tar.bz2", ".tar.bz", ".tar.tgz", ".tar", ".tgz", ".zip", ".rar"]

    def iszip(self, file):
        for z in self.compress:
            if file.endswith(z):
                return z

    def zip_to_path(self, file):
        for i in self.compress:
            file = file.replace(i, "")
        return file

    def error_record(self, info):
        with open("error.txt", "a+") as r:
            r.write(info + "\n")

    def un_zip(self, src, dst):
        """ src : aa/asdf.zip
            dst : unzip/aa/asdf.zip
        """
        try:
            zip_file = zipfile.ZipFile(src)
            uz_path = self.zip_to_path(dst)
            if not os.path.exists(uz_path):
                pass
            else:
                shutil.rmtree(uz_path)
            os.makedirs(uz_path)
            # for i in range(len(zip_file.namelist())):
            #    zip_file.namelist()[i] = zip_file.namelist()[i].encode('cp437').decode('gbk')

            with zipfile.ZipFile(src, 'r') as f:
                for fn in f.namelist():
                    xx = f.extract(fn, uz_path)
                    extracted_path = Path(xx)
                    yy = fn.encode('cp437').decode('gbk')
                    extracted_path.rename(uz_path + '/' + yy)
        except zipfile.BadZipfile:
            pass
        except zlib.error:
            print("zlib error : " + src)
            self.error_record("zlib error : " + src)

    def un_rar(self, src, dst):
        try:
            rar = unrar.rarfile.RarFile(src)
            uz_path = self.zip_to_path(dst)
            if not os.path.exists(uz_path):
                pass
            else:
                shutil.rmtree(uz_path)
            rar.extractall(uz_path)
        except unrar.rarfile.BadRarFile:
            pass
        except Exception as e:
            print(e)
            self.error_record(str(e) + src)

    def un_tar(self, src, dst):
        try:
            tar = tarfile.open(src)
            uz_path = self.zip_to_path(dst)
            if not os.path.exists(uz_path):
                pass
            else:
                shutil.rmtree(uz_path)
            tar.extractall(path=uz_path)
        except tarfile.ReadError:
            pass
        except Exception as e:
            print(e)
            self.error_record(str(e) + src)


class UnZip(BaseTool):
    """ UnZip files """

    def __init__(self, mode):
        super(UnZip, self).__init__(self)
        self.mode = mode
        self.output = "./data/unzip/"
        self.current_path = os.getcwd() + "/"

    def recursive_unzip(self, repath):
        """recursive unzip file 递归解压程序
        """
        for (root, dirs, files) in os.walk(repath):
            for filename in files:
                # if self.mode in filename:
                #     None
                # else:
                #     continue
                src = os.path.join(root, filename)
                if self.iszip(src) == ".zip":
                    print("[+] child unzip: " + src)
                    self.un_zip(src, src)
                    os.remove(src)
                    self.recursive_unzip(self.zip_to_path(src))
                    sleep(0.1)
                if self.iszip(src) == ".rar":
                    from unrar import rarfile
                    print("[+] child unrar : " + src)
                    self.un_rar(src, src)
                    os.remove(src)
                    self.recursive_unzip(self.zip_to_path(src))
                    sleep(0.1)
                if self.iszip(src) in (".tar.gz", ".tar.bz2", ".tar.bz", ".tar.tgz", ".tar", ".tgz"):
                    print("[+] child untar : " + src)
                    self.un_tar(src, src)
                    os.remove(src)
                    self.recursive_unzip(self.zip_to_path(src))
                    sleep(0.1)

    def remove_zip(self, repath):
        for (root, dirs, files) in os.walk(repath):
            for filename in files:
                if self.mode in filename:
                    None
                else:
                    continue
                src = os.path.join(root, filename)
                if self.iszip(src) == ".zip":
                    print("[-] remove zip: " + src)
                    os.remove(src)
                    sleep(0.1)  # 推迟调用线程的运行
                if self.iszip(src) == ".rar":
                    print("[-] remove rar: " + src)
                    os.remove(src)
                    sleep(0.1)
                if self.iszip(src) in (".tar.gz", ".tar.bz2", ".tar.bz", ".tar.tgz", ".tar", ".tgz"):
                    print("[-] remove tar: " + src)
                    os.remove(src)
                    sleep(0.1)

    def main_unzip(self, file_addr):
        file_name = os.path.split(file_addr)[1]
        # if self.mode in file_name:
        #     pass

        zippath = os.path.join(self.output)
        if not os.path.exists(zippath):
            os.makedirs(zippath)
        src = file_addr
        dst = os.path.join(self.output, file_name)
        if self.iszip(src) == ".zip":
            print("[+] main unzip : " + src)
            self.un_zip(src, dst)
        if self.iszip(src) == ".rar":
            print("[+] main unrar : " + src)
            self.un_rar(src, dst)
        if self.iszip(src) in (".tar.gz", ".tar.bz2", ".tar.bz", ".tar.tgz", ".tar", ".tgz"):
            print("[+] main untar : " + src)
            self.un_tar(src, dst)
        else:
            try:
                shutil.copyfile(src, dst)
            except OSError as e:
                print(str(e))
                self.error_record(str(e))

        self.recursive_unzip(self.output)
        # self.remove_zip(self.path)


    def main(self,my_model, file_addr):
        print("***********************************解压***********************************")
        z = UnZip(my_model)
        z.main_unzip(file_addr)
        return True
