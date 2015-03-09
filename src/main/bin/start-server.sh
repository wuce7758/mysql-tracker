#!/bin/sh

current_path = $(pwd)
# get the shell's father directory
case "$(uname)" in
    Linux)
        bin_abs_path=$(readlink -f $(dirname $0))
        ;;
    *)
        bin_abs_path=$(cd $(dirname $0); pwd)
        ;;
esac

base=${bin_abs_path}/..
conf=${base}/conf/tracker.properties
