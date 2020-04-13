#!/bin/bash
# Zsh and other crazy shells extends patterns passed arguments (glob), so be sure rsync runs in BASH!!!
CMD="rsync --chmod=Du=rwx,Dgo=rx,Fu=rw,Fgo=r --info=progress2 -ahz --info=progress2   ${1} -e ssh ngnms@185.178.84.143:${2}  ${3}"
echo ${CMD}
bash -c  "$CMD"
