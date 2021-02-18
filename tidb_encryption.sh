#!/usr/bin/env bash
# Copyright 2021 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# This script is used to checkout a TiDB PR branch in a forked repo.

printFunc(){
  echo -e "Usage:\n"
	echo -e "\t -e file/dir --- [compressing and encrypting]\n"
	echo -e "\t -u file ------- [uncompressing and unencrypting]\n"
	exit 0;
}

if [ $# -eq 2 ]; then
	echo ""
else
  printFunc
fi

opt=0
case "$1" in
  -e)
    echo -e "Found -e option \n"
    opt=1
    ;;
  -u)
    echo -e "Found -u option \n"
    opt=2
    ;;
  *)
    echo -e "$1 is not the option \n"
    printFunc
    ;;
esac
shift

type tar >/dev/null 2>&1 || { echo >&2 "Cmd tar is required but it's not installed.  Aborting."; exit 1; }
type openssl >/dev/null 2>&1 || { echo >&2 "Cmd openssl is required but it's not installed.  Aborting."; exit 1; }

if [ -e $1 ]; then
  echo ""
else
  printFunc
fi

backup_file=$1

if [ $opt -eq 1 ]; then
  echo "compressing and encrypting"
  tar -czvf - $backup_file | openssl enc -e -aes256 -out backup.tar.gz
else
  echo "uncompressing and unencrypting"
  openssl aes256 -d -in $backup_file | tar xzvf -
fi
